package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
)

const ORDER = 4

// MaxChildren 内部节点的孩子的个数
// assert MaxChildren + 1 >= 2 * MinChildren : 确保分裂后的两个Node 满足 MinChildren
const MaxChildren = 7

// MinChildren 内部节点的孩子的最少个数
// assert MinChildren >= 2
const MinChildren = 4

// MaxKeys 叶节点中的Key的最多个数
// assert MaxKeys + 1 >= 2 * MinKeys : 确保分裂后的两个 Node 满足 MinKeys
const MaxKeys = 3

// MinKeys 叶节点中的Key的最少个数
const MinKeys = 1

type UnionNode struct {
	parent   *UnionNode
	leftPtr  *UnionNode
	rightPtr *UnionNode

	isLeaf bool

	// internalNode field
	keys      []int
	childNum  int
	childPtrs []*UnionNode

	// leafNode field
	kvPairs []*KVPair
}

func newUnionNode(parent *UnionNode, leftPtr *UnionNode, rightPtr *UnionNode, isLeaf bool) *UnionNode {
	node := &UnionNode{
		parent:   parent,
		leftPtr:  leftPtr,
		rightPtr: rightPtr,

		isLeaf: isLeaf,
	}
	if isLeaf {
		// 由于实现是每次添加后检查是否大于 MaxKeys 再进行分裂的，所以 kvPairs 最多能达到 MaxKeys + 1
		node.kvPairs = make([]*KVPair, 0, MaxKeys+1)
	} else {
		// 由于实现是每次添加后检查是否大于MaxChildren再进行分裂的，所以 childPtrs 最多能达到 MaxChildren + 1
		node.keys = make([]int, 0, MaxChildren)
		node.childPtrs = make([]*UnionNode, 0, MaxChildren+1)
	}
	return node
}

func (lNode *UnionNode) insertKeyValue(t *BPlusTree, key int, value float64) {
	newKVPair := &KVPair{key, value}
	// 如果是空的或者大于最后一位key leafNode 直接添加后返回
	if len(lNode.kvPairs) == 0 || lNode.kvPairs[len(lNode.kvPairs)-1].key < key {
		lNode.kvPairs = append(lNode.kvPairs, newKVPair)
	} else {
		// 找到正确的位置，以便按顺序插入
		for i := 0; i < len(lNode.kvPairs); i++ {
			if lNode.kvPairs[i].key == key {
				lNode.kvPairs[i].value = value
				break
			}
			if lNode.kvPairs[i].key > key {
				lNode.kvPairs = append(lNode.kvPairs[:i], append([]*KVPair{newKVPair}, lNode.kvPairs[i:]...)...)
				break
			}
		}
	}

	// 是否需要分裂
	if len(lNode.kvPairs) > MaxKeys {
		lNode.split(t)
	}
}

func (iNode *UnionNode) insertNode(t *BPlusTree, key int, child *UnionNode) {
	insertKeyIdx := 0
	for ; insertKeyIdx < len(iNode.keys); insertKeyIdx++ {
		if iNode.keys[insertKeyIdx] > key {
			break
		}
	}
	iNode.keys = append(iNode.keys[:insertKeyIdx], append([]int{key}, iNode.keys[insertKeyIdx:]...)...)
	iNode.childPtrs = append(iNode.childPtrs[:insertKeyIdx+1], append([]*UnionNode{child}, iNode.childPtrs[insertKeyIdx+1:]...)...)
	iNode.childNum += 1
	if len(iNode.childPtrs) > MaxChildren {
		iNode.split(t)
	}
}
func (iNode *UnionNode) removeChild(t *BPlusTree, node *UnionNode) {
	for idx, childPtr := range iNode.childPtrs {
		if childPtr == node {
			if len(iNode.keys) <= 1 {
				iNode.childPtrs[idx] = nil
				if iNode.parent == nil {
					// root
					var n *UnionNode
					if iNode.childPtrs[0] != nil {
						n = iNode.childPtrs[0]
					} else {
						n = iNode.childPtrs[1]
					}

					if !n.isLeaf {
						t.root = n
					} else {
						t.root = nil
					}
					n.parent = nil

				}
			} else {
				if idx-1 == -1 {
					iNode.keys = iNode.keys[1:]
				} else {
					iNode.keys = append(iNode.keys[:idx-1], iNode.keys[idx:]...)
				}
				iNode.childPtrs = append(iNode.childPtrs[:idx], iNode.childPtrs[idx+1:]...)
			}
			iNode.childNum -= 1
			break
		}
	}

	if iNode.childNum < MinChildren {
		if !iNode.borrow() {
			iNode.merge(t)
		}
	}
}
func (lNode *UnionNode) removeKey(t *BPlusTree, key int) (removed bool) {
	for i := 0; i < len(lNode.kvPairs); i++ {
		if lNode.kvPairs[i].key == key {
			lNode.kvPairs = append(lNode.kvPairs[:i], lNode.kvPairs[i+1:]...)
			removed = true
			break
		}
	}

	if len(lNode.kvPairs) < MinKeys && lNode.parent != nil {
		if !lNode.borrow() {
			if len(lNode.kvPairs) == 0 {
				if lNode.leftPtr != nil {
					lNode.leftPtr.rightPtr = lNode.rightPtr
				}
				if lNode.rightPtr != nil {
					lNode.rightPtr.leftPtr = lNode.leftPtr
				}
				lNode.parent.removeChild(t, lNode)
			} else {
				// 如果借不到，那么他的直系兄弟的 key == MIN_KEYS,
				// 另外 len(lNode.kvPairs) < MIN_KEYS, 所以 len(lNode.kvPairs) + key <= 2 * MIN_KEY - 1 <= MAX_KEYS
				// 必定可以合并成功
				lNode.merge(t)
			}
		}
		// 如果是没有父节点的叶节点，那么整棵树也就只有它自己，不需要管了
	}
	return removed
}
func (node *UnionNode) split(t *BPlusTree) {
	if node.isLeaf {
		midKVIdx := len(node.kvPairs) / 2

		newLeafNode := newUnionNode(node.parent, node, node.rightPtr, true)
		newLeafNode.kvPairs = append(newLeafNode.kvPairs, node.kvPairs[midKVIdx:]...)

		node.kvPairs = node.kvPairs[:midKVIdx]
		if node.rightPtr != nil {
			node.rightPtr.leftPtr = newLeafNode
		}
		node.rightPtr = newLeafNode

		if node.parent == nil {
			parent := newUnionNode(nil, nil, nil, false)
			parent.keys = append(parent.keys, newLeafNode.kvPairs[0].key)
			parent.childPtrs = append(parent.childPtrs, node, newLeafNode)
			parent.childNum += 2
			node.parent = parent
			newLeafNode.parent = parent
			t.root = parent
		} else {
			node.parent.insertNode(t, newLeafNode.kvPairs[0].key, newLeafNode)
		}
	} else {
		// midKeyIdx 处的key移到父节点中，对应的child指针作为新节点左边的指针
		midKeyIdx := len(node.keys) / 2
		midKey := node.keys[midKeyIdx]

		newINode := newUnionNode(node.parent, node, node.rightPtr, false)
		// 新的节点只包含 midKeyIdx 后面的key
		newINode.keys = append(newINode.keys, node.keys[midKeyIdx+1:]...)
		newINode.childPtrs = append(newINode.childPtrs, node.childPtrs[midKeyIdx+1:]...)
		newINode.childNum += len(newINode.childPtrs)
		// 更改新节点的孩子的父节点为新节点
		for i := 0; i < len(newINode.childPtrs); i++ {
			newINode.childPtrs[i].parent = newINode
		}

		if node.rightPtr != nil {
			node.rightPtr.leftPtr = newINode
		}
		node.rightPtr = newINode
		node.keys = node.keys[:midKeyIdx]
		node.childPtrs = node.childPtrs[:midKeyIdx+1]
		node.childNum -= newINode.childNum

		// 原本的ChildPtr不再是同一个父节点，更改对应的指针为 nil
		if !node.isLeaf {
			node.childPtrs[len(node.childPtrs)-1].rightPtr = nil
			newINode.childPtrs[0].leftPtr = nil
		}

		if node.parent == nil {
			parent := newUnionNode(nil, nil, nil, false)
			parent.keys = append(parent.keys, midKey)
			parent.childPtrs = append(parent.childPtrs, node, newINode)
			parent.childNum += 2

			node.parent = parent
			newINode.parent = parent
			t.root = parent
		} else {
			node.parent.insertNode(t, midKey, newINode)
		}
	}
}

func (node *UnionNode) borrow() bool {
	if node.isLeaf {
		parent := node.parent
		if parent == nil {
			return false
		}
		lNodeInParentIdx := getIdxInParent(node)
		if node.leftPtr != nil && node.leftPtr.parent == parent && len(node.leftPtr.kvPairs) > MinKeys {
			// 先从左边借
			leftLeafNodeKVNum := len(node.leftPtr.kvPairs)
			node.kvPairs = append([]*KVPair{node.leftPtr.kvPairs[leftLeafNodeKVNum-1]}, node.kvPairs...)
			node.leftPtr.kvPairs = node.leftPtr.kvPairs[:leftLeafNodeKVNum-1]
			// 更新父节点指针对应的key的大小
			parent.keys[lNodeInParentIdx-1] = node.kvPairs[0].key
			return true
		} else if node.rightPtr != nil && node.rightPtr.parent == parent && len(node.rightPtr.kvPairs) > MinKeys {
			// 再从右边借e
			node.kvPairs = append(node.kvPairs, node.rightPtr.kvPairs[0])
			node.rightPtr.kvPairs = node.rightPtr.kvPairs[1:]
			// 更新父节点指针对应的key的大小
			parent.keys[lNodeInParentIdx] = node.rightPtr.kvPairs[0].key
			return true
		}
		return false
	} else {
		parent := node.parent
		if parent == nil {
			return false
		}
		var iNodeInParentIdx = getIdxInParent(node)
		leftINode := node.leftPtr
		rightINode := node.rightPtr
		if leftINode != nil && leftINode.parent == node.parent && len(leftINode.childPtrs) > MinChildren {
			// 向左借（右旋）
			midKey := parent.keys[iNodeInParentIdx-1]
			// 更改父节点 key
			parent.keys[iNodeInParentIdx-1] = leftINode.keys[len(leftINode.keys)-1]

			// 更改该节点 key childPtrs childNum属性
			node.keys = append([]int{midKey}, node.keys...)
			deleteNil(node)
			leftLastChildIdx := len(leftINode.childPtrs) - 1

			leftINode.childPtrs[leftLastChildIdx].parent = node
			node.childPtrs = append([]*UnionNode{leftINode.childPtrs[leftLastChildIdx]}, node.childPtrs...)
			node.childNum += 1

			// 更改左节点 key childPtrs childNum属性
			leftINode.keys = leftINode.keys[:len(leftINode.keys)-1]
			leftINode.childPtrs = leftINode.childPtrs[:len(leftINode.childPtrs)-1]
			leftINode.childNum -= 1

			// 更改孩子节点左右指针
			leftINode.childPtrs[len(leftINode.childPtrs)-1].rightPtr = nil
			node.childPtrs[0].leftPtr = nil
			node.childPtrs[0].rightPtr = node.childPtrs[1]
			node.childPtrs[1].leftPtr = node.childPtrs[0]

			return true
		} else if rightINode != nil && rightINode.parent == node.parent && len(rightINode.childPtrs) > MinChildren {
			// 向右借（左旋）
			midKey := parent.keys[iNodeInParentIdx]
			// 更改父节点 key
			parent.keys[iNodeInParentIdx] = rightINode.keys[0]

			// 更改该节点 key childPtrs childNum属性
			node.keys = append(node.keys, midKey)
			deleteNil(node)

			node.childPtrs = append(node.childPtrs, rightINode.childPtrs[0])
			rightINode.childPtrs[0].parent = node
			node.childNum += 1

			// 更改右节点 key childPtrs childNum属性
			rightINode.keys = rightINode.keys[1:]
			rightINode.childPtrs = rightINode.childPtrs[1:]
			rightINode.childNum -= 1

			// 更改孩子节点左右指针
			rightINode.childPtrs[0].leftPtr = nil
			node.childPtrs[len(node.childPtrs)-1].rightPtr = nil
			node.childPtrs[len(node.childPtrs)-1].leftPtr = node.childPtrs[len(node.childPtrs)-2]
			node.childPtrs[len(node.childPtrs)-2].rightPtr = node.childPtrs[len(node.childPtrs)-1]

			return true
		}
		return false
	}
}

func (node *UnionNode) merge(t *BPlusTree) {
	if node.isLeaf {
		// 进行合并, assert MIN_KEYS + (MIN_KEYS - 1) <= MAX_KEYS
		rightLeafNode := node.rightPtr
		leftLeafNode := node.leftPtr
		if leftLeafNode != nil && leftLeafNode.parent == node.parent && len(leftLeafNode.kvPairs) == MinKeys {
			// 这里的合并到左边
			leftLeafNode.kvPairs = append(leftLeafNode.kvPairs, node.kvPairs...)
			if rightLeafNode != nil {
				rightLeafNode.leftPtr = leftLeafNode
			}
			leftLeafNode.rightPtr = rightLeafNode
			leftLeafNode.parent.removeChild(t, node)
		} else if rightLeafNode != nil && rightLeafNode.parent == node.parent && len(rightLeafNode.kvPairs) == MinKeys {
			// 右边的合并到这里
			node.kvPairs = append(node.kvPairs, rightLeafNode.kvPairs...)
			node.rightPtr = rightLeafNode.rightPtr
			if rightLeafNode.rightPtr != nil {
				rightLeafNode.rightPtr.leftPtr = node
			}
			node.parent.removeChild(t, rightLeafNode)
		}
	} else {
		parent := node.parent
		if parent == nil {
			return
		}
		idxInParent := getIdxInParent(node)
		leftINode := node.leftPtr
		rightINode := node.rightPtr
		midKey := 0
		if leftINode != nil && leftINode.parent == parent && len(leftINode.childPtrs) == MinChildren {
			midKey = parent.keys[idxInParent-1]
			rightINode = node
			node.keys = append([]int{midKey}, rightINode.keys...)
		} else if rightINode != nil && rightINode.parent == parent && len(rightINode.childPtrs) == MinChildren {
			midKey = parent.keys[idxInParent]
			leftINode = node
			node.keys = append(leftINode.keys, midKey)
		} else {
			fmt.Println("in *internalNode merge")
			return // 无法合并
		}
		// 删除 nil 指针
		if len(node.childPtrs)-1 == node.childNum {
			for idx, childPtr := range node.childPtrs {
				if childPtr == nil {
					leftINode.keys = append(leftINode.keys[:idx], leftINode.keys[idx+1:]...)
					leftINode.childPtrs = append(leftINode.childPtrs[:idx], leftINode.childPtrs[idx+1:]...)
					break
				}
			}
		}

		// 更改子节点的父节点指向
		for _, childPtr := range rightINode.childPtrs {
			childPtr.parent = leftINode
		}

		// 右边合并到左边
		leftINode.keys = append(leftINode.keys, rightINode.keys...)
		leftINode.childPtrs = append(leftINode.childPtrs, rightINode.childPtrs...)

		leftINode.childPtrs[leftINode.childNum-1].rightPtr = leftINode.childPtrs[leftINode.childNum]
		leftINode.childPtrs[leftINode.childNum].leftPtr = leftINode.childPtrs[leftINode.childNum-1]

		// 更改 左右指针 childNum
		leftINode.rightPtr = rightINode.rightPtr
		if rightINode.rightPtr != nil {
			rightINode.rightPtr.leftPtr = leftINode
		}
		leftINode.childNum += rightINode.childNum

		parent.removeChild(t, rightINode)
	}
}

func (node *UnionNode) getMaxMinKey() (max int, min int) {
	if node.isLeaf {
		if len(node.kvPairs) == 0 {
			panic("Empty leafNode.")
		}
		max, min = node.kvPairs[0].key, node.kvPairs[0].key
		for i := 1; i < len(node.kvPairs); i++ {
			if node.kvPairs[i].key < min {
				min = node.kvPairs[i].key
			}
			if node.kvPairs[i].key > max {
				max = node.kvPairs[i].key
			}
		}
		return max, min
	} else {
		if len(node.childPtrs) < 2 {
			panic("Illegal InternalNode.")
		}
		max, min = math.MinInt, math.MaxInt
		for i := 1; i < len(node.childPtrs); i++ {
			if node.childPtrs[i] != nil {
				maxKey, minKey := node.childPtrs[i].getMaxMinKey()
				if maxKey > max {
					max = maxKey
				}
				if minKey < min {
					min = minKey
				}
			}
		}
		return max, min
	}
}

func getIdxInParent(node *UnionNode) int {
	if node.parent == nil {
		return -1
	}
	for idx, childPtr := range node.parent.childPtrs {
		if childPtr == node {
			return idx
		}
	}
	return -1
}

func deleteNil(iNode *UnionNode) {
	if iNode == nil || len(iNode.keys) != len(iNode.childPtrs) {
		return
	}
	for idx, childPtr := range iNode.childPtrs {
		if childPtr == nil {
			iNode.keys = append(iNode.keys[:idx], iNode.keys[idx+1:]...)
			iNode.childPtrs = append(iNode.childPtrs[:idx], iNode.childPtrs[idx+1:]...)
			break
		}
	}
}

type KVPair struct {
	key   int
	value float64
}

type BPlusTree struct {
	root          *UnionNode
	firstLeafNode *UnionNode
}

func MakeNew() *BPlusTree {
	return &BPlusTree{}
}

func (t *BPlusTree) Insert(key int, value float64) {
	t.findLeafNode(key).insertKeyValue(t, key, value)
	if !t.Diagnostic() {
		_ = fmt.Errorf("diagnostic failed after Insert")
		os.Exit(-1)
	}
}

func (t *BPlusTree) Delete(key int) bool {
	r := t.findLeafNode(key).removeKey(t, key)
	if !t.Diagnostic() {
		_ = fmt.Errorf("diagnostic failed after Delete")
		os.Exit(-1)
	}
	return r
}

func (t *BPlusTree) Get(key int) (float64, bool) {
	lNode := t.findLeafNode(key)
	for _, kvPair := range lNode.kvPairs {
		if kvPair.key == key {
			return kvPair.value, true
		}
	}
	return 0.0, false
}

// Diagnostic 检查结构是否正确
func (t *BPlusTree) Diagnostic() bool {
	if t.root != nil {

		queue := make([]*UnionNode, 0, 2*MaxChildren)
		queue = append(queue, t.root)

		var cursor *UnionNode
		for len(queue) > 0 {
			cursor = queue[0]
			queue = queue[1:]

			if !cursor.isLeaf {
				if cursor.childNum != len(cursor.childPtrs) {
					fmt.Println("错误：子节点数量不符。")
					return false
				}
				if cursor.parent != nil && (len(cursor.childPtrs) < MinChildren || MaxChildren < len(cursor.childPtrs)) {
					fmt.Println("错误：InternalNode 大小不合法。")
					return false
				}
				for idx, childPtr := range cursor.childPtrs {
					if childPtr == nil {
						fmt.Println("错误：不应有空指针的子节点")
						return false
					}
					if childPtr.parent != cursor {
						fmt.Println("错误：父子节点关系错误")
						return false
					}
					queue = append(queue, childPtr)
					if !childPtr.isLeaf {
						if idx == 0 {
							if childPtr.leftPtr != nil {
								fmt.Println("错误：不应有左兄弟")
								return false
							}
						}
						if idx == len(cursor.childPtrs)-1 {
							if childPtr.rightPtr != nil {
								fmt.Println("错误：不应有右兄弟")
								return false
							}
						}
						if childPtr.leftPtr != nil && childPtr.leftPtr.rightPtr != childPtr {
							fmt.Println("错误：左右兄弟不一致1")
							return false
						}
						if childPtr.rightPtr != nil && childPtr.rightPtr.leftPtr != childPtr {
							fmt.Println("错误：左右兄弟不一致2")
							return false
						}
					} else if childPtr.isLeaf {
						if childPtr.leftPtr != nil && childPtr.leftPtr.rightPtr != childPtr {
							fmt.Println("错误：左右兄弟不一致3")
							return false
						}
						if childPtr.rightPtr != nil && childPtr.rightPtr.leftPtr != childPtr {
							fmt.Println("错误：左右兄弟不一致4")
							return false
						}
					}

					maxKey, minKey := childPtr.getMaxMinKey()
					if idx-1 >= 0 && cursor.keys[idx-1] > minKey {
						fmt.Println("错误：子节点的最小值大于了父节点对应的key")
						return false
					}
					if idx < len(cursor.keys) && cursor.keys[idx] < maxKey {
						fmt.Println("错误：子节点的最大值大于了父节点对应的key的后面一个key")
						return false
					}
				}
			} else if cursor.isLeaf {
				if cursor.parent != nil && len(cursor.kvPairs) < MinKeys || MaxKeys < len(cursor.kvPairs) {
					fmt.Println("错误：LeafNode 大小不合法。")
					return false
				}
				for idx := range cursor.kvPairs {
					if idx-1 > 0 && cursor.kvPairs[idx].key < cursor.kvPairs[idx-1].key {
						fmt.Println("错误：KVPair内部顺寻不一致。")
						return false
					}
				}
			}
		}

	} else {
		// 没有root的话，最多只能有一个leafNode
		if t.firstLeafNode != nil {
			if t.firstLeafNode.parent != nil {
				fmt.Println("错误：不应有parent。")
				return false
			}
			if t.firstLeafNode.leftPtr != nil {
				fmt.Println("错误：不应有leftPtr。")
				return false
			}
			if t.firstLeafNode.rightPtr != nil {
				fmt.Println("错误：不应有rightPtr。")
				return false
			}
			for idx := range t.firstLeafNode.kvPairs {
				if idx > 0 && t.firstLeafNode.kvPairs[idx].key < t.firstLeafNode.kvPairs[idx-1].key {
					fmt.Println("错误：KVPair内部顺寻不一致。")
					return false
				}
			}
		}
	}
	return true
}

// 返回key所在的leafNode, 或者应该插入的leafNode
func (t *BPlusTree) findLeafNode(key int) (target *UnionNode) {
	if t.root == nil {
		if t.firstLeafNode == nil {
			t.firstLeafNode = newUnionNode(nil, nil, nil, true)
			return t.firstLeafNode
		}
		cursor := t.firstLeafNode
		for {
			for i := 0; i < len(cursor.kvPairs); i++ {
				if cursor.kvPairs[i].key >= key {
					return cursor
				}
			}
			if cursor.rightPtr == nil {
				return cursor
			}
			cursor = cursor.rightPtr
		}
	} else {
		cursor := t.root
		for {
			idx, found := slices.BinarySearch(cursor.keys, key)
			if cursor.childPtrs[0].isLeaf {
				if found {
					return cursor.childPtrs[idx+1]
				} else {
					return cursor.childPtrs[idx]
				}
			} else {
				if found {
					cursor = cursor.childPtrs[idx+1]
				} else {
					cursor = cursor.childPtrs[idx]
				}
			}
		}
	}
}

func main() {
	test(10000)
}

func test(n int) {
	m := make(map[int]float64)
	bPlusTree := MakeNew()

	n = int(math.Max(float64(n), 1000))

	fmt.Println("bPlusTree := MakeNew()")
	for i := 0; i < 1000; i++ {
		key := rand.Intn(1000)
		if _, ok := m[key]; ok {
			// 验证在BPlusTree中也存在
			if _, existInBPlusTree := bPlusTree.Get(key); !existInBPlusTree {
				fmt.Println("错误：应该存在该key")
			}

			// 已存在，则删除
			delete(m, key)
			fmt.Printf("bPlusTree.Delete(%v)\n", key)
			bPlusTree.Delete(key)

			// 验证在 BPlusTree 中已经不存在
			if _, existInBPlusTree := bPlusTree.Get(key); existInBPlusTree {
				fmt.Println("错误：删除后仍然存在")
			}
		} else {
			// 验证在 BPlusTree 中也不存在
			if _, existInBPlusTree := bPlusTree.Get(key); existInBPlusTree {
				fmt.Println("错误：不应该存在该key")
			}

			// 不存在，则添加
			m[key] = float64(key)
			fmt.Printf("bPlusTree.Insert(%v, float64(%v))\n", key, key)
			bPlusTree.Insert(key, float64(key))

			// 验证在 BPlusTree 中已经存在
			if _, existInBPlusTree := bPlusTree.Get(key); !existInBPlusTree {
				fmt.Println("错误: 添加后不在")
			}
		}
	}
}

func test1() {

	bPlusTree := MakeNew()

	//1 4 7 10 17 21 31 25 19 20 28 42
	bPlusTree.Insert(1, 1)
	fmt.Println(bPlusTree.Get(1))
	bPlusTree.Insert(4, 4)
	fmt.Println(bPlusTree.Get(4))
	bPlusTree.Insert(7, 7)
	fmt.Println(bPlusTree.Get(7))
	bPlusTree.Insert(10, 10)
	fmt.Println(bPlusTree.Get(10))
	bPlusTree.Insert(17, 17)
	fmt.Println(bPlusTree.Get(17))
	bPlusTree.Insert(21, 21)
	fmt.Println(bPlusTree.Get(21))
	bPlusTree.Insert(31, 31)
	fmt.Println(bPlusTree.Get(31))
	bPlusTree.Insert(25, 25)
	fmt.Println(bPlusTree.Get(25))
	bPlusTree.Insert(19, 19)
	fmt.Println(bPlusTree.Get(19))
	bPlusTree.Insert(20, 20)
	fmt.Println(bPlusTree.Get(20))
	bPlusTree.Insert(28, 28)
	fmt.Println(bPlusTree.Get(28))
	bPlusTree.Insert(42, 42)
	fmt.Println(bPlusTree.Get(42))

	bPlusTree.Delete(21)
	fmt.Println(bPlusTree.Get(21))
	bPlusTree.Delete(31)
	fmt.Println(bPlusTree.Get(31))
	bPlusTree.Delete(20)
	fmt.Println(bPlusTree.Get(20))
	bPlusTree.Delete(4)
	fmt.Println(bPlusTree.Get(4))
}
