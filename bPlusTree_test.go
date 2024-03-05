package main

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestBPlusTree(t *testing.T) {
	m := make(map[int]float64)
	bPlusTree := MakeNew()
	n := 1000
	fmt.Println("bPlusTree := MakeNew()")
	for i := 0; i < n; i++ {
		key := rand.Intn(1000)
		if _, ok := m[key]; ok {
			// 验证在BPlusTree中也存在
			if _, existInBPlusTree := bPlusTree.Get(key); !existInBPlusTree {
				t.Errorf("错误: 应该存在该key")
			}

			// 已存在，则删除
			delete(m, key)
			fmt.Printf("bPlusTree.Delete(%v)\n", key)
			bPlusTree.Delete(key)
			if !Diagnose(bPlusTree) {
				t.Errorf("diagnostic failed after Delete")
			}

			// 验证在 BPlusTree 中已经不存在
			if _, existInBPlusTree := bPlusTree.Get(key); existInBPlusTree {
				t.Errorf("错误: 删除后仍然存在")
			}
		} else {
			// 验证在 BPlusTree 中也不存在
			if _, existInBPlusTree := bPlusTree.Get(key); existInBPlusTree {
				t.Errorf("错误: 不应该存在该key")
			}

			// 不存在，则添加
			m[key] = float64(key)
			fmt.Printf("bPlusTree.Insert(%v, float64(%v))\n", key, key)
			bPlusTree.Insert(key, float64(key))
			if !Diagnose(bPlusTree) {
				t.Errorf("diagnostic failed after Insert")
			}

			// 验证在 BPlusTree 中已经存在
			if _, existInBPlusTree := bPlusTree.Get(key); !existInBPlusTree {
				t.Errorf("错误: 添加后不在")
			}
		}
	}
}

// Diagnose 检查结构是否正确
func Diagnose(t *BPlusTree) bool {
	if t.root != nil {
		queue := make([]*UnionNode, 0, 2*MaxChildren)
		queue = append(queue, t.root)
		var cursor *UnionNode
		for len(queue) > 0 {
			cursor = queue[0]
			queue = queue[1:]
			if !cursor.isLeaf {
				if cursor.childNum != len(cursor.children) {
					fmt.Println("错误: 子节点数量不符。")
					return false
				}
				if cursor.parent != nil && (len(cursor.children) < MinChildren || MaxChildren < len(cursor.children)) {
					fmt.Println("错误: InternalNode 大小不合法。")
					return false
				}
				for idx, childPtr := range cursor.children {
					if childPtr == nil {
						fmt.Println("错误: 不应有空指针的子节点")
						return false
					}
					if childPtr.parent != cursor {
						fmt.Println("错误: 父子节点关系错误")
						return false
					}
					queue = append(queue, childPtr)
					if !childPtr.isLeaf {
						if idx == 0 {
							if childPtr.leftPtr != nil {
								fmt.Println("错误: 不应有左兄弟")
								return false
							}
						}
						if idx == len(cursor.children)-1 {
							if childPtr.rightPtr != nil {
								fmt.Println("错误: 不应有右兄弟")
								return false
							}
						}
						if childPtr.leftPtr != nil && childPtr.leftPtr.rightPtr != childPtr {
							fmt.Println("错误: 左右兄弟不一致1")
							return false
						}
						if childPtr.rightPtr != nil && childPtr.rightPtr.leftPtr != childPtr {
							fmt.Println("错误: 左右兄弟不一致2")
							return false
						}
					} else if childPtr.isLeaf {
						if childPtr.leftPtr != nil && childPtr.leftPtr.rightPtr != childPtr {
							fmt.Println("错误: 左右兄弟不一致3")
							return false
						}
						if childPtr.rightPtr != nil && childPtr.rightPtr.leftPtr != childPtr {
							fmt.Println("错误: 左右兄弟不一致4")
							return false
						}
					}

					maxKey, minKey := childPtr.getMaxMinKey()
					if idx-1 >= 0 && cursor.keys[idx-1] > minKey {
						fmt.Println("错误: 子节点的最小值大于了父节点对应的key")
						return false
					}
					if idx < len(cursor.keys) && cursor.keys[idx] < maxKey {
						fmt.Println("错误: 子节点的最大值大于了父节点对应的key的后面一个key")
						return false
					}
				}
			} else if cursor.isLeaf {
				if cursor.parent != nil && len(cursor.kvPairs) < MinKeys || MaxKeys < len(cursor.kvPairs) {
					fmt.Println("错误: LeafNode 大小不合法。")
					return false
				}
				for idx := range cursor.kvPairs {
					if idx-1 > 0 && cursor.kvPairs[idx].key < cursor.kvPairs[idx-1].key {
						fmt.Println("错误: KVPair内部顺寻不一致。")
						return false
					}
				}
			}
		}

	} else {
		// 没有root的话，最多只能有一个leafNode
		if t.firstLeafNode != nil {
			if t.firstLeafNode.parent != nil {
				fmt.Println("错误: 不应有parent。")
				return false
			}
			if t.firstLeafNode.leftPtr != nil {
				fmt.Println("错误: 不应有leftPtr。")
				return false
			}
			if t.firstLeafNode.rightPtr != nil {
				fmt.Println("错误: 不应有rightPtr。")
				return false
			}
			for idx := range t.firstLeafNode.kvPairs {
				if idx > 0 && t.firstLeafNode.kvPairs[idx].key < t.firstLeafNode.kvPairs[idx-1].key {
					fmt.Println("错误: KVPair内部顺寻不一致。")
					return false
				}
			}
		}
	}
	return true
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
		if len(node.children) < 2 {
			panic("Illegal InternalNode.")
		}
		max, min = math.MinInt, math.MaxInt
		for i := 1; i < len(node.children); i++ {
			maxKey, minKey := node.children[i].getMaxMinKey()
			if maxKey > max {
				max = maxKey
			}
			if minKey < min {
				min = minKey
			}
		}
		return max, min
	}
}
