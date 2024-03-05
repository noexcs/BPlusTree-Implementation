package main

import (
	"slices"
)

// MaxChildren 内部节点的孩子的个数，
// 又称 Order (used by Knuth's definition)
// MaxChildren + 1 >= 2 * MinChildren : 确保分裂后的两个Node 满足 MinChildren
const MaxChildren = 7

// MinChildren 内部节点的孩子的最少个数，
// 又称 Degree (used in the definition in Cormen et al. in Introduction to Algorithms (CLRS))
// MinChildren + (MinChildren - 1) <= MaxChildren : 确保合并后的可能的最大孩子数量不超过 MaxChildren
const MinChildren = 4

// MaxKeys 叶节点中的Key的最多个数
// MaxKeys + 1 >= 2 * MinKeys : 确保分裂后的两个 Node 满足 MinKeys
const MaxKeys = 3

// MinKeys 叶节点中的Key的最少个数
// MinKey + (MinKeys - 1) <= MaxKeys : 确保合并后的可能的最大孩子数量不超过 MaxKeys
const MinKeys = 1

type UnionNode struct {
	parent   *UnionNode
	leftPtr  *UnionNode
	rightPtr *UnionNode

	isLeaf bool

	// internalNode field
	keys     []int
	childNum int
	children []*UnionNode

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
		// 由于实现是每次添加后检查是否大于 MaxChildren 再进行分裂的，所以 children 最多能达到 MaxChildren + 1
		node.keys = make([]int, 0, MaxChildren)
		node.children = make([]*UnionNode, 0, MaxChildren+1)
	}
	return node
}

func (node *UnionNode) insertKeyValue(t *BPlusTree, key int, value any) {
	idx, found := slices.BinarySearchFunc(node.kvPairs, key, func(kvPair *KVPair, k int) int {
		return kvPair.key - k
	})
	if found {
		node.kvPairs[idx].value = value
	} else {
		node.kvPairs = append(node.kvPairs[:idx], append([]*KVPair{{key, value}}, node.kvPairs[idx:]...)...)
	}
	// 是否需要分裂
	if len(node.kvPairs) > MaxKeys {
		node.split(t)
	}
}

func (node *UnionNode) insertNode(t *BPlusTree, key int, childNode *UnionNode) {
	insertKeyIdx, _ := slices.BinarySearchFunc(node.keys, key, func(keyInNode int, k int) int {
		return keyInNode - k
	})
	node.keys = append(node.keys[:insertKeyIdx], append([]int{key}, node.keys[insertKeyIdx:]...)...)
	node.children = append(node.children[:insertKeyIdx+1], append([]*UnionNode{childNode}, node.children[insertKeyIdx+1:]...)...)
	node.childNum += 1
	if len(node.children) > MaxChildren {
		node.split(t)
	}
}
func (node *UnionNode) removeChild(t *BPlusTree, childNode *UnionNode) {
	for idx, childPtr := range node.children {
		if childPtr == childNode {
			if len(node.keys) <= 1 {
				node.children[idx] = nil
				if node.parent == nil {
					// root
					var n *UnionNode
					if node.children[0] != nil {
						n = node.children[0]
					} else {
						n = node.children[1]
					}
					t.root = n
					n.parent = nil
				}
			} else {
				if idx-1 == -1 {
					node.keys = node.keys[1:]
				} else {
					node.keys = append(node.keys[:idx-1], node.keys[idx:]...)
				}
				node.children = append(node.children[:idx], node.children[idx+1:]...)
			}
			node.childNum -= 1
			break
		}
	}
	if node != t.root && node.childNum < MinChildren {
		if !node.borrow() {
			node.merge(t)
		}
	}
}
func (node *UnionNode) removeKey(t *BPlusTree, key int) bool {
	idx, found := slices.BinarySearchFunc(node.kvPairs, key, func(kvPair *KVPair, k int) int {
		return kvPair.key - k
	})
	if found {
		node.kvPairs = append(node.kvPairs[:idx], node.kvPairs[idx+1:]...)
	}
	if len(node.kvPairs) < MinKeys && node.parent != nil {
		if !node.borrow() {
			if len(node.kvPairs) == 0 {
				if node.leftPtr != nil {
					node.leftPtr.rightPtr = node.rightPtr
				}
				if node.rightPtr != nil {
					node.rightPtr.leftPtr = node.leftPtr
				}
				node.parent.removeChild(t, node)
			} else {
				node.merge(t)
			}
		}
	}
	return found
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
			parent.children = append(parent.children, node, newLeafNode)
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
		newINode.children = append(newINode.children, node.children[midKeyIdx+1:]...)
		newINode.childNum += len(newINode.children)
		// 更改新节点的孩子的父节点为新节点
		for i := 0; i < len(newINode.children); i++ {
			newINode.children[i].parent = newINode
		}

		if node.rightPtr != nil {
			node.rightPtr.leftPtr = newINode
		}
		node.rightPtr = newINode
		node.keys = node.keys[:midKeyIdx]
		node.children = node.children[:midKeyIdx+1]
		node.childNum -= newINode.childNum

		// 原本的 ChildPtr 不再是同一个父节点，更改对应的指针为 nil
		if !node.isLeaf {
			node.children[len(node.children)-1].rightPtr = nil
			newINode.children[0].leftPtr = nil
		}

		if node.parent == nil {
			parent := newUnionNode(nil, nil, nil, false)
			parent.keys = append(parent.keys, midKey)
			parent.children = append(parent.children, node, newINode)
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
	parent := node.parent
	if parent == nil {
		return false
	}
	if node.isLeaf {
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
		var iNodeInParentIdx = getIdxInParent(node)
		leftINode := node.leftPtr
		rightINode := node.rightPtr
		if leftINode != nil && leftINode.parent == node.parent && len(leftINode.children) > MinChildren {
			// 向左借（右旋）
			midKey := parent.keys[iNodeInParentIdx-1]
			// 更改父节点 key
			parent.keys[iNodeInParentIdx-1] = leftINode.keys[len(leftINode.keys)-1]

			// 更改该节点 key children childNum属性
			node.keys = append([]int{midKey}, node.keys...)
			deleteNil(node)
			leftLastChildIdx := len(leftINode.children) - 1

			leftINode.children[leftLastChildIdx].parent = node
			node.children = append([]*UnionNode{leftINode.children[leftLastChildIdx]}, node.children...)
			node.childNum += 1

			// 更改左节点 key children childNum属性
			leftINode.keys = leftINode.keys[:len(leftINode.keys)-1]
			leftINode.children = leftINode.children[:len(leftINode.children)-1]
			leftINode.childNum -= 1

			// 更改孩子节点左右指针
			leftINode.children[len(leftINode.children)-1].rightPtr = nil
			node.children[0].leftPtr = nil
			node.children[0].rightPtr = node.children[1]
			node.children[1].leftPtr = node.children[0]

			return true
		} else if rightINode != nil && rightINode.parent == node.parent && len(rightINode.children) > MinChildren {
			// 向右借（左旋）
			midKey := parent.keys[iNodeInParentIdx]
			// 更改父节点 key
			parent.keys[iNodeInParentIdx] = rightINode.keys[0]

			// 更改该节点 key children childNum属性
			node.keys = append(node.keys, midKey)
			deleteNil(node)

			node.children = append(node.children, rightINode.children[0])
			rightINode.children[0].parent = node
			node.childNum += 1

			// 更改右节点 key children childNum属性
			rightINode.keys = rightINode.keys[1:]
			rightINode.children = rightINode.children[1:]
			rightINode.childNum -= 1

			// 更改孩子节点左右指针
			rightINode.children[0].leftPtr = nil
			node.children[len(node.children)-1].rightPtr = nil
			node.children[len(node.children)-1].leftPtr = node.children[len(node.children)-2]
			node.children[len(node.children)-2].rightPtr = node.children[len(node.children)-1]

			return true
		}
		return false
	}
}

func (node *UnionNode) merge(t *BPlusTree) {
	parent := node.parent
	if parent == nil {
		return
	}
	if node.isLeaf {
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
		idxInParent := getIdxInParent(node)
		leftINode := node.leftPtr
		rightINode := node.rightPtr
		midKey := 0
		if leftINode != nil && leftINode.parent == parent && len(leftINode.children) == MinChildren {
			midKey = parent.keys[idxInParent-1]
			rightINode = node
			node.keys = append([]int{midKey}, rightINode.keys...)
		} else if rightINode != nil && rightINode.parent == parent && len(rightINode.children) == MinChildren {
			midKey = parent.keys[idxInParent]
			leftINode = node
			node.keys = append(leftINode.keys, midKey)
		} else {
			// 无法合并
			panic("In *Union.merge error")
		}
		// 删除 nil 指针
		if len(node.children)-1 == node.childNum {
			deleteNil(node)
		}

		// 更改子节点的父节点指向
		for _, childPtr := range rightINode.children {
			childPtr.parent = leftINode
		}

		// 右边合并到左边
		leftINode.keys = append(leftINode.keys, rightINode.keys...)
		leftINode.children = append(leftINode.children, rightINode.children...)

		leftINode.children[leftINode.childNum-1].rightPtr = leftINode.children[leftINode.childNum]
		leftINode.children[leftINode.childNum].leftPtr = leftINode.children[leftINode.childNum-1]

		// 更改 左右指针 childNum
		leftINode.rightPtr = rightINode.rightPtr
		if rightINode.rightPtr != nil {
			rightINode.rightPtr.leftPtr = leftINode
		}
		leftINode.childNum += rightINode.childNum

		parent.removeChild(t, rightINode)
	}
}

func getIdxInParent(node *UnionNode) int {
	if node.parent == nil {
		return -1
	}
	for idx, childPtr := range node.parent.children {
		if childPtr == node {
			return idx
		}
	}
	return -1
}

func deleteNil(node *UnionNode) {
	if node == nil || len(node.keys) != len(node.children) {
		return
	}
	for idx, childPtr := range node.children {
		if childPtr == nil {
			node.keys = append(node.keys[:idx], node.keys[idx+1:]...)
			node.children = append(node.children[:idx], node.children[idx+1:]...)
			break
		}
	}
}

type KVPair struct {
	key   int
	value any
}

type BPlusTree struct {
	root *UnionNode
}

func MakeNew() *BPlusTree {
	return &BPlusTree{}
}

func (t *BPlusTree) Insert(key int, value any) {
	t.findLeafNode(key).insertKeyValue(t, key, value)
}

func (t *BPlusTree) Delete(key int) bool {
	return t.findLeafNode(key).removeKey(t, key)
}

func (t *BPlusTree) Get(key int) (any, bool) {
	leafNode := t.findLeafNode(key)
	idx, found := slices.BinarySearchFunc(leafNode.kvPairs, key, func(kvPair *KVPair, k int) int {
		return kvPair.key - k
	})
	if found {
		return leafNode.kvPairs[idx].value, true
	}
	return nil, false
}

// 返回key所在的leafNode, 或者应该插入的leafNode
func (t *BPlusTree) findLeafNode(key int) (target *UnionNode) {
	if t.root == nil {
		t.root = newUnionNode(nil, nil, nil, true)
		return t.root
	}
	if t.root.isLeaf {
		return t.root
	}
	cursor := t.root
	for {
		idx, found := slices.BinarySearch(cursor.keys, key)
		if found {
			cursor = cursor.children[idx+1]
		} else {
			cursor = cursor.children[idx]
		}
		if cursor.isLeaf {
			return cursor
		}
	}
}
