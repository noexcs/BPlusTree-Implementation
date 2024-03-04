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

// Node *internalNode | *leafNode
type Node interface {
	getMaxMinKey() (max int, min int)
}

type internalNode struct {
	parent    *internalNode
	leftPtr   *internalNode
	rightPtr  *internalNode
	keys      []int
	childNum  int
	childPtrs []Node
}

func (iNode *internalNode) getMaxMinKey() (max int, min int) {
	if len(iNode.childPtrs) < 2 {
		panic("Illegal InternalNode.")
	}
	max, min = math.MinInt, math.MaxInt
	for i := 1; i < len(iNode.childPtrs); i++ {
		if iNode.childPtrs[i] != nil {
			maxKey, minKey := iNode.childPtrs[i].getMaxMinKey()
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

func makeNewInternalNode(parent *internalNode, leftPtr *internalNode, rightPtr *internalNode) *internalNode {
	return &internalNode{
		parent:   parent,
		leftPtr:  leftPtr,
		rightPtr: rightPtr,
		// 由于实现是每次添加后检查是否大于MaxChildren再进行分裂的，所以 childPtrs 最多能达到 MaxChildren + 1
		keys:      make([]int, 0, MaxChildren),
		childPtrs: make([]Node, 0, MaxChildren+1),
	}
}

func (iNode *internalNode) insert(t *BPlusTree, key int, child Node) {
	insertKeyIdx := 0
	for ; insertKeyIdx < len(iNode.keys); insertKeyIdx++ {
		if iNode.keys[insertKeyIdx] > key {
			break
		}
	}
	iNode.keys = append(iNode.keys[:insertKeyIdx], append([]int{key}, iNode.keys[insertKeyIdx:]...)...)
	if _, ok := child.(*internalNode); ok {
		iNode.childPtrs = append(iNode.childPtrs[:insertKeyIdx+1], append([]Node{child.(*internalNode)}, iNode.childPtrs[insertKeyIdx+1:]...)...)
		iNode.childNum += 1
	}
	if _, ok := child.(*leafNode); ok {
		iNode.childPtrs = append(iNode.childPtrs[:insertKeyIdx+1], append([]Node{child.(*leafNode)}, iNode.childPtrs[insertKeyIdx+1:]...)...)
		iNode.childNum += 1
	}

	if len(iNode.childPtrs) > MaxChildren {
		iNode.split(t)
	}
}

func (iNode *internalNode) split(t *BPlusTree) {
	// midKeyIdx 处的key移到父节点中，对应的child指针作为新节点左边的指针
	midKeyIdx := len(iNode.keys) / 2
	midKey := iNode.keys[midKeyIdx]

	newINode := makeNewInternalNode(iNode.parent, iNode, iNode.rightPtr)
	// 新的节点只包含 midKeyIdx 后面的key
	newINode.keys = append(newINode.keys, iNode.keys[midKeyIdx+1:]...)
	newINode.childPtrs = append(newINode.childPtrs, iNode.childPtrs[midKeyIdx+1:]...)
	newINode.childNum += len(newINode.childPtrs)
	// 更改新节点的孩子的父节点为新节点
	for i := 0; i < len(newINode.childPtrs); i++ {
		if childNode, ok := newINode.childPtrs[i].(*internalNode); ok {
			childNode.parent = newINode
		}
		if childNode, ok := newINode.childPtrs[i].(*leafNode); ok {
			childNode.parent = newINode
		}
	}

	if iNode.rightPtr != nil {
		iNode.rightPtr.leftPtr = newINode
	}
	iNode.rightPtr = newINode
	iNode.keys = iNode.keys[:midKeyIdx]
	iNode.childPtrs = iNode.childPtrs[:midKeyIdx+1]
	iNode.childNum -= newINode.childNum

	// 原本的ChildPtr不再是同一个父节点，更改对应的指针为 nil
	if iNodeChild, ok := iNode.childPtrs[len(iNode.childPtrs)-1].(*internalNode); ok {
		iNodeChild.rightPtr = nil
	}
	if iNodeChild, ok := newINode.childPtrs[0].(*internalNode); ok {
		iNodeChild.leftPtr = nil
	}

	if iNode.parent == nil {
		parent := makeNewInternalNode(nil, nil, nil)
		parent.keys = append(parent.keys, midKey)
		parent.childPtrs = append(parent.childPtrs, iNode, newINode)
		parent.childNum += 2

		iNode.parent = parent
		newINode.parent = parent
		t.root = parent
	} else {
		iNode.parent.insert(t, midKey, newINode)
	}
}

func (iNode *internalNode) removeChild(t *BPlusTree, node Node) {
	for idx, childPtr := range iNode.childPtrs {
		if childPtr == node {
			if len(iNode.keys) <= 1 {
				iNode.childPtrs[idx] = nil
				if iNode.parent == nil {
					// root
					var n Node
					if iNode.childPtrs[0] != nil {
						n = iNode.childPtrs[0]
					} else {
						n = iNode.childPtrs[1]
					}
					if _, ok := n.(*internalNode); ok {
						t.root = n.(*internalNode)
						n.(*internalNode).parent = nil
					} else if _, ok := n.(*leafNode); ok {
						t.root = nil
						n.(*leafNode).parent = nil
					}
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
		if !iNode.borrow(t) {
			iNode.merge(t)
		}
	}
}

func (iNode *internalNode) merge(t *BPlusTree) {
	parent := iNode.parent
	if parent == nil {
		return
	}
	idxInParent := getIdxInParent(iNode)
	leftINode := iNode.leftPtr
	rightINode := iNode.rightPtr
	midKey := 0
	if leftINode != nil && leftINode.parent == parent && len(leftINode.childPtrs) == MinChildren {
		midKey = parent.keys[idxInParent-1]
		rightINode = iNode
		iNode.keys = append([]int{midKey}, rightINode.keys...)
	} else if rightINode != nil && rightINode.parent == parent && len(rightINode.childPtrs) == MinChildren {
		midKey = parent.keys[idxInParent]
		leftINode = iNode
		iNode.keys = append(leftINode.keys, midKey)
	} else {
		fmt.Println("in *internalNode merge")
		return // 无法合并
	}
	// 删除 nil 指针
	if len(iNode.childPtrs)-1 == iNode.childNum {
		for idx, childPtr := range iNode.childPtrs {
			if childPtr == nil {
				leftINode.keys = append(leftINode.keys[:idx], leftINode.keys[idx+1:]...)
				leftINode.childPtrs = append(leftINode.childPtrs[:idx], leftINode.childPtrs[idx+1:]...)
				break
			}
		}
	}

	// 更改子节点的父节点指向
	for _, childPtr := range rightINode.childPtrs {
		if newINode, ok := childPtr.(*internalNode); ok {
			newINode.parent = leftINode
		} else if newLNode, ok := childPtr.(*leafNode); ok {
			newLNode.parent = leftINode
		}
	}

	// 右边合并到左边
	leftINode.keys = append(leftINode.keys, rightINode.keys...)
	leftINode.childPtrs = append(leftINode.childPtrs, rightINode.childPtrs...)

	if _, ok := leftINode.childPtrs[leftINode.childNum-1].(*internalNode); ok {
		leftINode.childPtrs[leftINode.childNum-1].(*internalNode).rightPtr = leftINode.childPtrs[leftINode.childNum].(*internalNode)
		leftINode.childPtrs[leftINode.childNum].(*internalNode).leftPtr = leftINode.childPtrs[leftINode.childNum-1].(*internalNode)
	} else {
		leftINode.childPtrs[leftINode.childNum-1].(*leafNode).rightPtr = leftINode.childPtrs[leftINode.childNum].(*leafNode)
		leftINode.childPtrs[leftINode.childNum].(*leafNode).leftPtr = leftINode.childPtrs[leftINode.childNum-1].(*leafNode)
	}

	// 更改 左右指针 childNum
	leftINode.rightPtr = rightINode.rightPtr
	if rightINode.rightPtr != nil {
		rightINode.rightPtr.leftPtr = leftINode
	}
	leftINode.childNum += rightINode.childNum

	parent.removeChild(t, rightINode)
}

func getIdxInParent(node Node) int {
	if iNode, ok := node.(*internalNode); ok {
		if iNode.parent == nil {
			return -1
		}
		for idx, childPtr := range iNode.parent.childPtrs {
			if childPtr == iNode {
				return idx
			}
		}
	} else if lNode, ok := node.(*leafNode); ok {
		if lNode.parent == nil {
			return -1
		}
		for idx, childPtr := range lNode.parent.childPtrs {
			if childPtr == lNode {
				return idx
			}
		}
	}
	return -1
}

func deleteNil(iNode *internalNode) {
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

func (iNode *internalNode) borrow(t *BPlusTree) bool {
	parent := iNode.parent
	if parent == nil {
		return false
	}
	var iNodeInParentIdx = getIdxInParent(iNode)
	leftINode := iNode.leftPtr
	rightINode := iNode.rightPtr
	if leftINode != nil && leftINode.parent == iNode.parent && len(leftINode.childPtrs) > MinChildren {
		// 向左借（右旋）
		midKey := parent.keys[iNodeInParentIdx-1]
		// 更改父节点 key
		parent.keys[iNodeInParentIdx-1] = leftINode.keys[len(leftINode.keys)-1]

		// 更改该节点 key childPtrs childNum属性
		iNode.keys = append([]int{midKey}, iNode.keys...)
		deleteNil(iNode)
		leftLastChildIdx := len(leftINode.childPtrs) - 1
		if newINode, ok := leftINode.childPtrs[leftLastChildIdx].(*internalNode); ok {
			newINode.parent = iNode
			iNode.childPtrs = append([]Node{newINode}, iNode.childPtrs...)
			iNode.childNum += 1
		} else if newLNode, ok := leftINode.childPtrs[leftLastChildIdx].(*leafNode); ok {
			newLNode.parent = iNode
			iNode.childPtrs = append([]Node{newLNode}, iNode.childPtrs...)
			iNode.childNum += 1
		}

		// 更改左节点 key childPtrs childNum属性
		leftINode.keys = leftINode.keys[:len(leftINode.keys)-1]
		leftINode.childPtrs = leftINode.childPtrs[:len(leftINode.childPtrs)-1]
		leftINode.childNum -= 1

		// 更改孩子节点左右指针
		if _, ok := leftINode.childPtrs[len(leftINode.childPtrs)-1].(*internalNode); ok {
			leftINode.childPtrs[len(leftINode.childPtrs)-1].(*internalNode).rightPtr = nil
			iNode.childPtrs[0].(*internalNode).leftPtr = nil
			iNode.childPtrs[0].(*internalNode).rightPtr = iNode.childPtrs[1].(*internalNode)
			iNode.childPtrs[1].(*internalNode).leftPtr = iNode.childPtrs[0].(*internalNode)
		} else if _, ok := leftINode.childPtrs[len(leftINode.childPtrs)-1].(*leafNode); ok {
			leftINode.childPtrs[len(leftINode.childPtrs)-1].(*leafNode).rightPtr = nil
			iNode.childPtrs[0].(*leafNode).leftPtr = nil
			iNode.childPtrs[0].(*leafNode).rightPtr = iNode.childPtrs[1].(*leafNode)
			iNode.childPtrs[1].(*leafNode).leftPtr = iNode.childPtrs[0].(*leafNode)
		}
		return true
	} else if rightINode != nil && rightINode.parent == iNode.parent && len(rightINode.childPtrs) > MinChildren {
		// 向右借（左旋）
		midKey := parent.keys[iNodeInParentIdx]
		// 更改父节点 key
		parent.keys[iNodeInParentIdx] = rightINode.keys[0]

		// 更改该节点 key childPtrs childNum属性
		iNode.keys = append(iNode.keys, midKey)
		deleteNil(iNode)

		iNode.childPtrs = append(iNode.childPtrs, rightINode.childPtrs[0])
		if newINode, ok := rightINode.childPtrs[0].(*internalNode); ok {
			newINode.parent = iNode
		} else if newLNode, ok := rightINode.childPtrs[0].(*leafNode); ok {
			newLNode.parent = iNode
		}
		iNode.childNum += 1

		// 更改右节点 key childPtrs childNum属性
		rightINode.keys = rightINode.keys[1:]
		rightINode.childPtrs = rightINode.childPtrs[1:]
		rightINode.childNum -= 1

		// 更改孩子节点左右指针
		if _, ok := rightINode.childPtrs[0].(*internalNode); ok {
			rightINode.childPtrs[0].(*internalNode).leftPtr = nil
			iNode.childPtrs[len(iNode.childPtrs)-1].(*internalNode).rightPtr = nil
			iNode.childPtrs[len(iNode.childPtrs)-1].(*internalNode).leftPtr = iNode.childPtrs[len(iNode.childPtrs)-2].(*internalNode)
			iNode.childPtrs[len(iNode.childPtrs)-2].(*internalNode).rightPtr = iNode.childPtrs[len(iNode.childPtrs)-1].(*internalNode)
		} else if _, ok := rightINode.childPtrs[0].(*leafNode); ok {
			rightINode.childPtrs[0].(*leafNode).leftPtr = nil
			iNode.childPtrs[len(iNode.childPtrs)-1].(*leafNode).rightPtr = nil
			iNode.childPtrs[len(iNode.childPtrs)-1].(*leafNode).leftPtr = iNode.childPtrs[len(iNode.childPtrs)-2].(*leafNode)
			iNode.childPtrs[len(iNode.childPtrs)-2].(*leafNode).rightPtr = iNode.childPtrs[len(iNode.childPtrs)-1].(*leafNode)
		}
		return true
	}
	return false
}

type leafNode struct {
	parent   *internalNode
	leftPtr  *leafNode
	rightPtr *leafNode
	kvPairs  []*KVPair
}

func (lNode *leafNode) getMaxMinKey() (max int, min int) {
	if len(lNode.kvPairs) == 0 {
		panic("Empty leafNode.")
	}
	max, min = lNode.kvPairs[0].key, lNode.kvPairs[0].key
	for i := 1; i < len(lNode.kvPairs); i++ {
		if lNode.kvPairs[i].key < min {
			min = lNode.kvPairs[i].key
		}
		if lNode.kvPairs[i].key > max {
			max = lNode.kvPairs[i].key
		}
	}
	return max, min
}

func newLeafNode(parent *internalNode, leftPtr *leafNode, rightPtr *leafNode) *leafNode {
	return &leafNode{
		parent:   parent,
		leftPtr:  leftPtr,
		rightPtr: rightPtr,
		// 由于实现是每次添加后检查是否大于 MaxKeys 再进行分裂的，所以 kvPairs 最多能达到 MaxKeys + 1
		kvPairs: make([]*KVPair, 0, MaxKeys+1),
	}
}

func (lNode *leafNode) insert(t *BPlusTree, key int, value float64) {
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

func (lNode *leafNode) split(t *BPlusTree) {

	midKVIdx := len(lNode.kvPairs) / 2

	newLeafNode := newLeafNode(lNode.parent, lNode, lNode.rightPtr)
	newLeafNode.kvPairs = append(newLeafNode.kvPairs, lNode.kvPairs[midKVIdx:]...)

	lNode.kvPairs = lNode.kvPairs[:midKVIdx]
	if lNode.rightPtr != nil {
		lNode.rightPtr.leftPtr = newLeafNode
	}
	lNode.rightPtr = newLeafNode

	if lNode.parent == nil {
		parent := makeNewInternalNode(nil, nil, nil)
		parent.keys = append(parent.keys, newLeafNode.kvPairs[0].key)
		parent.childPtrs = append(parent.childPtrs, lNode, newLeafNode)
		parent.childNum += 2
		lNode.parent = parent
		newLeafNode.parent = parent
		t.root = parent
	} else {
		lNode.parent.insert(t, newLeafNode.kvPairs[0].key, newLeafNode)
	}
}

func (lNode *leafNode) borrow() bool {
	parent := lNode.parent
	if parent == nil {
		return false
	}
	lNodeInParentIdx := getIdxInParent(lNode)
	if lNode.leftPtr != nil && lNode.leftPtr.parent == parent && len(lNode.leftPtr.kvPairs) > MinKeys {
		// 先从左边借
		leftLeafNodeKVNum := len(lNode.leftPtr.kvPairs)
		lNode.kvPairs = append([]*KVPair{lNode.leftPtr.kvPairs[leftLeafNodeKVNum-1]}, lNode.kvPairs...)
		lNode.leftPtr.kvPairs = lNode.leftPtr.kvPairs[:leftLeafNodeKVNum-1]
		// 更新父节点指针对应的key的大小
		parent.keys[lNodeInParentIdx-1] = lNode.kvPairs[0].key
		return true
	} else if lNode.rightPtr != nil && lNode.rightPtr.parent == parent && len(lNode.rightPtr.kvPairs) > MinKeys {
		// 再从右边借e
		lNode.kvPairs = append(lNode.kvPairs, lNode.rightPtr.kvPairs[0])
		lNode.rightPtr.kvPairs = lNode.rightPtr.kvPairs[1:]
		// 更新父节点指针对应的key的大小
		parent.keys[lNodeInParentIdx] = lNode.rightPtr.kvPairs[0].key
		return true
	}
	return false
}
func (lNode *leafNode) merge(t *BPlusTree) (mergedNode *leafNode) {
	// 进行合并, assert MIN_KEYS + (MIN_KEYS - 1) <= MAX_KEYS
	rightLeafNode := lNode.rightPtr
	leftLeafNode := lNode.leftPtr
	if leftLeafNode != nil && leftLeafNode.parent == lNode.parent && len(leftLeafNode.kvPairs) == MinKeys {
		// 这里的合并到左边
		leftLeafNode.kvPairs = append(leftLeafNode.kvPairs, lNode.kvPairs...)
		if rightLeafNode != nil {
			rightLeafNode.leftPtr = leftLeafNode
		}
		leftLeafNode.rightPtr = rightLeafNode
		leftLeafNode.parent.removeChild(t, lNode)
	} else if rightLeafNode != nil && rightLeafNode.parent == lNode.parent && len(rightLeafNode.kvPairs) == MinKeys {
		// 右边的合并到这里
		lNode.kvPairs = append(lNode.kvPairs, rightLeafNode.kvPairs...)
		lNode.rightPtr = rightLeafNode.rightPtr
		if rightLeafNode.rightPtr != nil {
			rightLeafNode.rightPtr.leftPtr = lNode
		}
		lNode.parent.removeChild(t, rightLeafNode)
	}
	return
}

func (lNode *leafNode) removeKey(t *BPlusTree, key int) (removed bool) {
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

type KVPair struct {
	key   int
	value float64
}

type BPlusTree struct {
	root          *internalNode
	firstLeafNode *leafNode
}

func MakeNew() *BPlusTree {
	return &BPlusTree{}
}

func (t *BPlusTree) Insert(key int, value float64) {
	t.findLeafNode(key).insert(t, key, value)
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

		queue := make([]Node, 0, 2*MaxChildren)
		queue = append(queue, t.root)

		var cursor Node
		for len(queue) > 0 {
			cursor = queue[0]
			queue = queue[1:]

			if iNode, ok := cursor.(*internalNode); ok {
				if iNode.childNum != len(iNode.childPtrs) {
					fmt.Println("错误：子节点数量不符。")
					return false
				}
				if iNode.parent != nil && (len(iNode.childPtrs) < MinChildren || MaxChildren < len(iNode.childPtrs)) {
					fmt.Println("错误：InternalNode 大小不合法。")
					return false
				}
				for idx, childPtr := range iNode.childPtrs {
					if childPtr == nil {
						fmt.Println("错误：不应有空指针的子节点")
						return false
					}
					if childINode, ok := childPtr.(*internalNode); ok {
						if childINode.parent != iNode {
							fmt.Println("错误：父子节点关系错误")
							return false
						}
					} else if childLNode, ok := childPtr.(*internalNode); ok {
						if childLNode.parent != iNode {
							fmt.Println("错误：父子节点关系错误")
							return false
						}
					}
					queue = append(queue, childPtr)
					if iNodeChild, ok := childPtr.(*internalNode); ok {
						if idx == 0 {
							if iNodeChild.leftPtr != nil {
								fmt.Println("错误：不应有左兄弟")
								return false
							}
						}
						if idx == len(iNode.childPtrs)-1 {
							if iNodeChild.rightPtr != nil {
								fmt.Println("错误：不应有右兄弟")
								return false
							}
						}
						if iNodeChild.leftPtr != nil && iNodeChild.leftPtr.rightPtr != iNodeChild {
							fmt.Println("错误：左右兄弟不一致1")
							return false
						}
						if iNodeChild.rightPtr != nil && iNodeChild.rightPtr.leftPtr != iNodeChild {
							fmt.Println("错误：左右兄弟不一致2")
							return false
						}
					} else if lNodeChild, ok := childPtr.(*leafNode); ok {
						if lNodeChild.leftPtr != nil && lNodeChild.leftPtr.rightPtr != lNodeChild {
							fmt.Println("错误：左右兄弟不一致3")
							return false
						}
						if lNodeChild.rightPtr != nil && lNodeChild.rightPtr.leftPtr != lNodeChild {
							fmt.Println("错误：左右兄弟不一致4")
							return false
						}
					}

					maxKey, minKey := childPtr.getMaxMinKey()
					if idx-1 >= 0 && iNode.keys[idx-1] > minKey {
						fmt.Println("错误：子节点的最小值大于了父节点对应的key")
						return false
					}
					if idx < len(iNode.keys) && iNode.keys[idx] < maxKey {
						fmt.Println("错误：子节点的最大值大于了父节点对应的key的后面一个key")
						return false
					}
				}
			} else if lNode, ok := cursor.(*leafNode); ok {
				if lNode.parent != nil && len(lNode.kvPairs) < MinKeys || MaxKeys < len(lNode.kvPairs) {
					fmt.Println("错误：LeafNode 大小不合法。")
					return false
				}
				for idx := range lNode.kvPairs {
					if idx-1 > 0 && lNode.kvPairs[idx].key < lNode.kvPairs[idx-1].key {
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

type Iterator struct {
	leafNode *leafNode
	index    int
}

func (iter *Iterator) Next() bool {
	return iter.leafNode != nil && iter.index < len(iter.leafNode.kvPairs)
}

func (iter *Iterator) Value() (key int, value float64) {
	if !iter.Next() {
		panic("No more element in this iterator.")
	}
	key = iter.leafNode.kvPairs[iter.index].key
	value = iter.leafNode.kvPairs[iter.index].value
	if iter.index+1 < len(iter.leafNode.kvPairs) {
		iter.index += 1
	} else {
		iter.leafNode = iter.leafNode.rightPtr
		iter.index = 0
	}
	return key, value
}

func (t *BPlusTree) Iterator() *Iterator {
	return &Iterator{leafNode: t.firstLeafNode, index: 0}
}

// 返回key所在的leafNode, 或者应该插入的leafNode
func (t *BPlusTree) findLeafNode(key int) (target *leafNode) {
	if t.root == nil {
		if t.firstLeafNode == nil {
			t.firstLeafNode = newLeafNode(nil, nil, nil)
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
			_, isLeaf := cursor.childPtrs[0].(*leafNode)
			if isLeaf {
				if found {
					return cursor.childPtrs[idx+1].(*leafNode)
				} else {
					return cursor.childPtrs[idx].(*leafNode)
				}
			} else {
				if found {
					cursor = cursor.childPtrs[idx+1].(*internalNode)
				} else {
					cursor = cursor.childPtrs[idx].(*internalNode)
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
