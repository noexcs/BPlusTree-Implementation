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
	if len(node.children) > MaxChildren {
		node.split(t)
	}
}

func (node *UnionNode) removeChild(t *BPlusTree, childNode *UnionNode) {
	for idx, childPtr := range node.children {
		if childPtr == childNode {
			if idx-1 == -1 {
				node.keys = node.keys[1:]
			} else {
				node.keys = append(node.keys[:idx-1], node.keys[idx:]...)
			}
			node.children = append(node.children[:idx], node.children[idx+1:]...)
			if node.parent == t.root && len(node.children) == 1 {
				t.root = node.children[0]
				t.root.parent = nil
			}
			break
		}
	}
	if node != t.root && len(node.children) < MinChildren {
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
	if node != t.root && len(node.kvPairs) < MinKeys {
		if !node.borrow() {
			node.merge(t)
		}
	}
	return found
}

func (node *UnionNode) split(t *BPlusTree) {
	newNode := newUnionNode(node.parent, node, node.rightPtr, node.isLeaf)
	midKey := 0
	if node.isLeaf {
		midKeyIdx := len(node.kvPairs) / 2
		midKey = node.kvPairs[midKeyIdx].key
		newNode.kvPairs = append(newNode.kvPairs, node.kvPairs[midKeyIdx:]...)
		node.kvPairs = node.kvPairs[:midKeyIdx]
	} else {
		// midKeyIdx 处的key移到父节点中，对应的child指针作为新节点左边的指针
		midKeyIdx := len(node.keys) / 2
		midKey = node.keys[midKeyIdx]

		// 新的节点只包含 midKeyIdx 后面的key
		newNode.keys = append(newNode.keys, node.keys[midKeyIdx+1:]...)
		newNode.children = append(newNode.children, node.children[midKeyIdx+1:]...)
		// 更改新节点的孩子的父节点为新节点
		for i := 0; i < len(newNode.children); i++ {
			newNode.children[i].parent = newNode
		}

		node.keys = node.keys[:midKeyIdx]
		node.children = node.children[:midKeyIdx+1]

		// 原本的 ChildPtr 不再是同一个父节点，更改对应的指针为 nil
		if !node.children[len(node.children)-1].isLeaf {
			node.children[len(node.children)-1].rightPtr = nil
			newNode.children[0].leftPtr = nil
		}
	}
	if node.rightPtr != nil {
		node.rightPtr.leftPtr = newNode
	}
	node.rightPtr = newNode
	if node.parent == nil {
		parent := newUnionNode(nil, nil, nil, false)
		parent.keys = append(parent.keys, midKey)
		parent.children = append(parent.children, node, newNode)

		node.parent = parent
		newNode.parent = parent
		t.root = parent
	} else {
		node.parent.insertNode(t, midKey, newNode)
	}
}

func (node *UnionNode) borrow() bool {
	parent := node.parent
	leftNodePtr := node.leftPtr
	rightNodePtr := node.rightPtr
	idxInParent := getIdxInParent(node)
	if node.isLeaf {
		if borrowable(leftNodePtr, node) {
			// 先从左边借
			node.kvPairs = append([]*KVPair{leftNodePtr.kvPairs[len(leftNodePtr.kvPairs)-1]}, node.kvPairs...)
			leftNodePtr.kvPairs = leftNodePtr.kvPairs[:len(leftNodePtr.kvPairs)-1]
			// 更新父节点指针对应的 key 的大小
			parent.keys[idxInParent-1] = node.kvPairs[0].key
			return true
		} else if borrowable(rightNodePtr, node) {
			// 再从右边借
			node.kvPairs = append(node.kvPairs, rightNodePtr.kvPairs[0])
			rightNodePtr.kvPairs = rightNodePtr.kvPairs[1:]
			// 更新父节点指针对应的key的大小
			parent.keys[idxInParent] = rightNodePtr.kvPairs[0].key
			return true
		}
	} else {
		if borrowable(leftNodePtr, node) {
			// 向左借（右旋）
			midKey := parent.keys[idxInParent-1]
			// 更改父节点 key
			parent.keys[idxInParent-1] = leftNodePtr.keys[len(leftNodePtr.keys)-1]

			// 更改该节点 key children 属性
			node.keys = append([]int{midKey}, node.keys...)
			node.children = append([]*UnionNode{leftNodePtr.children[len(leftNodePtr.children)-1]}, node.children...)

			// 更改左节点 key children 属性
			leftNodePtr.keys = leftNodePtr.keys[:len(leftNodePtr.keys)-1]
			leftNodePtr.children = leftNodePtr.children[:len(leftNodePtr.children)-1]

			// 更改孩子节点父节点指针，以及左右指针
			leftNodePtr.children[len(leftNodePtr.children)-1].rightPtr = nil
			node.children[0].parent = node
			node.children[0].leftPtr = nil
			node.children[0].rightPtr = node.children[1]
			node.children[1].leftPtr = node.children[0]

			return true
		} else if borrowable(rightNodePtr, node) {
			// 向右借（左旋）
			midKey := parent.keys[idxInParent]
			// 更改父节点 key
			parent.keys[idxInParent] = rightNodePtr.keys[0]

			// 更改该节点 key children 属性
			node.keys = append(node.keys, midKey)
			node.children = append(node.children, rightNodePtr.children[0])

			// 更改右节点 key children 属性
			rightNodePtr.keys = rightNodePtr.keys[1:]
			rightNodePtr.children = rightNodePtr.children[1:]

			// 更改孩子节点父节点指针，以及左右指针
			rightNodePtr.children[0].leftPtr = nil
			node.children[len(node.children)-1].parent = node
			node.children[len(node.children)-1].rightPtr = nil
			node.children[len(node.children)-1].leftPtr = node.children[len(node.children)-2]
			node.children[len(node.children)-2].rightPtr = node.children[len(node.children)-1]

			return true
		}
	}
	return false
}

func (node *UnionNode) merge(t *BPlusTree) {
	parent := node.parent
	leftNodePtr := node.leftPtr
	rightNodePtr := node.rightPtr

	if mergeable(leftNodePtr) {
		// 这里的合并到左边
		rightNodePtr = node
		if !node.isLeaf {
			leftNodePtr.keys = append(leftNodePtr.keys, parent.keys[getIdxInParent(rightNodePtr)-1])
		}
	} else if mergeable(rightNodePtr) {
		// 右边的合并到这里
		leftNodePtr = node
		if !node.isLeaf {
			leftNodePtr.keys = append(leftNodePtr.keys, parent.keys[getIdxInParent(leftNodePtr)])
		}
	}

	if node.isLeaf {
		leftNodePtr.kvPairs = append(leftNodePtr.kvPairs, rightNodePtr.kvPairs...)
	} else {
		for _, childPtr := range rightNodePtr.children {
			childPtr.parent = leftNodePtr
		}
		leftNodePtr.keys = append(leftNodePtr.keys, rightNodePtr.keys...)

		leftNodePtr.children[len(leftNodePtr.children)-1].rightPtr = rightNodePtr.children[0]
		rightNodePtr.children[0].leftPtr = leftNodePtr.children[len(leftNodePtr.children)-1]
		leftNodePtr.children = append(leftNodePtr.children, rightNodePtr.children...)
	}

	leftNodePtr.rightPtr = rightNodePtr.rightPtr
	if rightNodePtr.rightPtr != nil {
		rightNodePtr.rightPtr.leftPtr = leftNodePtr
	}
	parent.removeChild(t, rightNodePtr)
}

func mergeable(node *UnionNode) bool {
	if node == nil {
		return false
	}
	if node.isLeaf {
		return len(node.kvPairs) == MinKeys
	} else {
		return len(node.children) == MinChildren
	}
}

func borrowable(node *UnionNode, borrower *UnionNode) bool {
	if node == nil || borrower == nil {
		return false
	}
	if node.isLeaf {
		return node.parent == borrower.parent && len(node.kvPairs) > MinKeys
	} else {
		return len(node.children) > MinChildren
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

type KVPair struct {
	key   int
	value any
}

type BPlusTree struct {
	root          *UnionNode
	firstLeafNode *UnionNode
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
		t.firstLeafNode = newUnionNode(nil, nil, nil, true)
		t.root = t.firstLeafNode
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

func (t *BPlusTree) Iterator() *Iterator {
	return &Iterator{
		node: t.firstLeafNode,
		idx:  0,
	}
}

type Iterator struct {
	node *UnionNode
	idx  int
}

func (iter *Iterator) Next() bool {
	return iter.node != nil && iter.idx < len(iter.node.kvPairs)
}

func (iter *Iterator) Value() (k int, v any) {
	if !iter.Next() {
		panic("No more elements.")
	}
	kvPair := iter.node.kvPairs[iter.idx]
	if iter.idx+1 < len(iter.node.kvPairs) {
		iter.idx += 1
	} else {
		iter.node = iter.node.rightPtr
		iter.idx = 0
	}
	return kvPair.key, kvPair.value
}
