package partition_split

import (
	"container/heap"
)

// ============================================== map[Vertex]int 返回value值最大的10个key
// HeapItem 是堆中的元素，包含key和value
type HeapItem struct {
	key   Vertex
	value int // value of the item; arbitrary, changeabl
	index int // The index of the item in the heap.
}

// PriorityQue implements heap.Interface and holds HeapItems.
type PriorityQue []*HeapItem

func (pq PriorityQue) Len() int { return len(pq) }

func (pq PriorityQue) Less(i, j int) bool {
	// We want Pop to give us the lowest, not highest, priority so we use greater than here.
	return pq[i].value < pq[j].value
}

func (pq PriorityQue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*HeapItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an HeapItem in the queue.
func (pq *PriorityQue) update(item *HeapItem, value int) {
	item.value = value
	heap.Fix(pq, item.index)
}

// topCLargestKeys 返回map中value最大的10个key
func topCLargestKeys(m map[Vertex]int, c int) []Vertex {
	if c <= 0 {
		return nil
	}
	if len(m) < c {
		//fmt.Println("分片内节点数不够", c, len(m))
		c = len(m)
	}
	pq := make(PriorityQue, 0)
	heap.Init(&pq)

	// 遍历map并构建堆
	for key, value := range m {
		if pq.Len() < c {
			heap.Push(&pq, &HeapItem{value: value, key: key})
		} else if value > pq[0].value {
			heap.Pop(&pq)
			heap.Push(&pq, &HeapItem{value: value, key: key})
		}
	}

	// 堆中的元素即为最大的k个key
	keys := make([]Vertex, pq.Len())
	for i := pq.Len() - 1; i >= 0; i-- {
		item := heap.Pop(&pq).(*HeapItem)
		keys[i] = item.key
	}
	return keys
}

//========================================================================================

// Vertex 定义为 int 类型，你可以根据实际需求调整
//type Vertex int

// Edge 定义为 []Vertex 类型，用于表示图的边
//type Edge []Vertex

// Item 定义了堆中的元素，包含 Vertex 和其对应的 []Vertex 长度
type Item struct {
	vertex Vertex
	length int
}

// ByLength 实现了 heap.Interface 接口，用于维护一个小顶堆
type ByLength []Item

func (h ByLength) Len() int           { return len(h) }
func (h ByLength) Less(i, j int) bool { return h[i].length < h[j].length }
func (h ByLength) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *ByLength) Push(x interface{}) {
	*h = append(*h, x.(Item))
}

func (h *ByLength) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// getTop10LongestEdges 找出 map[Vertex][]Vertex 中 []Vertex 长度最长的前n个 Vertex
func getTopNLongestEdges(graph map[Vertex][]Vertex, n int) []Vertex {
	h := &ByLength{}
	heap.Init(h)

	for vertex, edges := range graph {
		edgeLength := len(edges)
		if h.Len() < n {
			heap.Push(h, Item{vertex, edgeLength})
		} else if edgeLength > (*h)[0].length {
			heap.Pop(h)
			heap.Push(h, Item{vertex, edgeLength})
		}
	}

	result := make([]Vertex, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(Item).vertex
	}
	return result
}
