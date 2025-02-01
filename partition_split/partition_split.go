package partition_split

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"fmt"
	"log"
	"math"
)

// CLPA算法状态，state of constraint label propagation algorithm
type ABState struct {
	NetGraph Graph // 需运行CLPA算法的图

	//ShardVertexListMap map[int][]Vertex // 分片ID → 节点List 分片内有哪些节点
	VertexAlloMap     map[Vertex]int // 节点 → 分片ID
	VertexLoadMap     map[Vertex]int // 节点 → 边数
	ShardCrossEdgeNum []int          // 分片ID → 跨片边数
	ShardInnerEdgeNum []int          // 分片ID → 片内边数
	ShardEdgeNum      []int          // 分片ID → 边数
	ShardVertexNum    []int          // 分片ID → 节点数

	//EdgeSetWithWeight map[Vertex]map[Vertex]int

	WeightPenalty     float64 // 权重惩罚，对应论文中的 beta
	MinShardEdgeNum   int     // 最少边数的分片ID
	AvgShardEdgeNum   float64 // 分片平均边数目
	MaxIterations     int     // 最大迭代次数，constraint，对应论文中的\tau
	CrossShardEdgeNum int     // 跨分片边的总数
	ShardNum          int     // 分片数目
	GraphHash         []byte

	thisAvgScore float64
	thisTotScore float64
}

// 设置参数
func (cs *ABState) Init_ABState(wp float64, mIter, sn int) {
	cs.WeightPenalty = wp
	cs.MaxIterations = mIter
	cs.ShardNum = sn
	cs.ShardVertexNum = make([]int, cs.ShardNum)
	cs.VertexAlloMap = make(map[Vertex]int)
	cs.VertexLoadMap = make(map[Vertex]int)
	//cs.ShardVertexListMap = make(map[int][]Vertex)
	cs.ShardCrossEdgeNum = make([]int, cs.ShardNum)
	cs.ShardInnerEdgeNum = make([]int, cs.ShardNum)
	cs.ShardEdgeNum = make([]int, cs.ShardNum)
	//cs.EdgeSetWithWeight = make(map[Vertex]map[Vertex]int)
}

// 加入节点，需要将它默认归到一个分片中
func (cs *ABState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v)
	if val, ok := cs.VertexAlloMap[v]; !ok {
		cs.VertexAlloMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		cs.VertexAlloMap[v] = val
	}
	cs.ShardVertexNum[cs.VertexAlloMap[v]] += 1 // 此处可以批处理完之后再修改 VertexsNumInShard 参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数
}

// 加入边，需要将它的端点（如果不存在）默认归到一个分片中
func (cs *ABState) AddEdge(u, v Vertex) {
	// 如果没有点，则增加边，权恒定为 1
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v)
	// 可以批处理完之后再修改 Edges2Shard 等参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数
}

func (cs *ABState) InitConfig() {
	// init VertexLoadMap 节点的边数
	for v, uList := range cs.NetGraph.EdgeSet {
		cs.VertexLoadMap[v] = len(uList)
	}
	//init ShardCrossEdgeNum和ShardInnerEdgeNum 跨片边和片内边
	cs.ShardCrossEdgeNum = make([]int, cs.ShardNum)
	cs.ShardInnerEdgeNum = make([]int, cs.ShardNum)
	for v, uList := range cs.NetGraph.EdgeSet {
		vShard := cs.VertexAlloMap[v]
		for _, u := range uList {
			uShard := cs.VertexAlloMap[u]
			if vShard != uShard {
				// 判断节点 v, u 不属于同一分片，则对应的 ShardCrossEdgeNum 加一
				// 仅计算入度，这样不会重复计算
				cs.ShardCrossEdgeNum[vShard] += 1
			} else {
				cs.ShardInnerEdgeNum[vShard] += 1
			}
		}
	}
	// init CrossShardEdgeNum
	cs.CrossShardEdgeNum = 0
	for _, val := range cs.ShardCrossEdgeNum {
		cs.CrossShardEdgeNum += val
	}
	//cs.CrossShardEdgeNum /= 2 //无向边，得/2
	// 分片负载=片内边+跨片边
	//cs.ShardCrossEdgeNum[ShardID]+cs.ShardInnerEdgeNum[ShardID]
	cs.ShardEdgeNum = make([]int, cs.ShardNum)
	for i := 0; i < cs.ShardNum; i++ {
		cs.ShardInnerEdgeNum[i] /= 2
		cs.ShardEdgeNum[i] = cs.ShardCrossEdgeNum[i] + cs.ShardInnerEdgeNum[i]
	}
}

// 在账户所属分片变动时，重新计算各个参数，faster
func (cs *ABState) ShardLoadRecompute(v Vertex, oldShardID int) {
	newShardID := cs.VertexAlloMap[v]
	cs.ShardVertexNum[newShardID] += 1
	cs.ShardVertexNum[oldShardID] -= 1
	for _, u := range cs.NetGraph.EdgeSet[v] { // 找到v所有邻接点u
		uShard := cs.VertexAlloMap[u]
		if uShard == oldShardID { // u属于老分片
			//老分片：片内边 → 跨片边 片内边-1 跨片边+1
			cs.ShardInnerEdgeNum[oldShardID] -= 1
			cs.ShardCrossEdgeNum[oldShardID] += 1
			//新分片：无关边 → 跨片边 边数+=1
			cs.ShardCrossEdgeNum[newShardID] += 1
			// 因此跨片边 +=2
			cs.CrossShardEdgeNum += 2
		}
		if uShard == newShardID { // u属于新分片
			//老分片：跨片边 → 无关边 跨片边 -=w
			cs.ShardCrossEdgeNum[oldShardID] -= 1
			//新分片：跨片边 → 片内边 跨片边-1 片内边+1
			cs.ShardCrossEdgeNum[newShardID] -= 1
			cs.ShardInnerEdgeNum[newShardID] += 1
			// 因此跨片边-=2
			cs.CrossShardEdgeNum -= 2
		}
		if uShard != newShardID && uShard != oldShardID {
			// 既不属于新分片，又不属于老分片，无片内边，跨片边数目不变
			//老分片的跨片边 → 新分片的跨片边
			cs.ShardCrossEdgeNum[newShardID] += 1
			cs.ShardCrossEdgeNum[oldShardID] -= 1
		}
	}
	for i := 0; i < cs.ShardNum; i++ {
		cs.ShardEdgeNum[i] = cs.ShardCrossEdgeNum[i] + cs.ShardInnerEdgeNum[i]
	}
	cs.MinShardEdgeNum = 0x7ffffffff
	// 修改 MinShardEdgeNum, CrossShardEdgeNum
	cs.AvgShardEdgeNum = 0.0
	for _, val := range cs.ShardEdgeNum {
		if cs.MinShardEdgeNum > val {
			cs.MinShardEdgeNum = val
		}
		cs.AvgShardEdgeNum += float64(val)
	}
	cs.AvgShardEdgeNum /= float64(cs.ShardNum)
}

// ********************************* AB
func (cs *ABState) getCorrelationScore(v Vertex) (int, float64) {
	shardScore := make([]float64, cs.ShardNum)
	connToShard := make([]int, cs.ShardNum)
	oldShardID := cs.VertexAlloMap[v]
	v_connNum := len(cs.NetGraph.EdgeSet[v]) // v的所有邻接点个数

	for _, u := range cs.NetGraph.EdgeSet[v] { // 找到v所有邻接点u
		uShard := cs.VertexAlloMap[u]
		connToShard[uShard] += 1 // v与分片uShard的关联度++
		// 如果v移动到uShard
	}
	// score的变化
	maxScore := -9999.0
	var maxScoreShard int
	for i := 0; i < cs.ShardNum; i++ {
		shardScore[i] = (float64(connToShard[i]) / float64(v_connNum)) *
			math.Pow(cs.AvgShardEdgeNum/float64(cs.ShardEdgeNum[i]), params.ScorePower)
		//log.Println("================== ", cs.AvgShardEdgeNum, float64(cs.ShardEdgeNum[i]),
		//	cs.AvgShardEdgeNum/float64(cs.ShardEdgeNum[i]),
		//	math.Pow(cs.AvgShardEdgeNum/float64(cs.ShardEdgeNum[i]), params.ScorePower), shardScore[i])
		if maxScore < shardScore[i] {
			maxScore = shardScore[i]
			maxScoreShard = i
		}
	}
	scoreIncrement := maxScore - shardScore[oldShardID]
	return maxScoreShard, scoreIncrement
}

func (cs *ABState) ABPartition() map[string]uint64 {
	cs.InitConfig()
	if len(cs.NetGraph.EdgeSet) == 0 {
		log.Panic("partition_split.ABPartition:  EdgeSet未累加，为空...")
	}
	result := make(map[string]uint64)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		//cs.thisTotScore = 0.0
		for v := range cs.NetGraph.VertexSet { // 遍历所有点
			oldShard := cs.VertexAlloMap[v]
			bestShard, _ := cs.getCorrelationScore(v)
			if oldShard != bestShard && cs.ShardVertexNum[oldShard] > 1 {
				cs.VertexAlloMap[v] = bestShard    // 将v分配到max_scoreShard
				result[v.Addr] = uint64(bestShard) // PartitionMap和res记录的相同的东西
				cs.ShardLoadRecompute(v, oldShard)
			}
		}
	}

	fmt.Println()
	fmt.Println("********** 划分结果 **********")
	for sid := 0; sid < cs.ShardNum; sid++ {
		fmt.Println("Shard", sid, "vertexNum:", cs.ShardVertexNum[sid],
			"edgeNum", cs.ShardEdgeNum[sid], "= inner", cs.ShardInnerEdgeNum[sid], "+ cross", cs.ShardCrossEdgeNum[sid])
	}
	fmt.Println("跨片边数：", cs.CrossShardEdgeNum)
	fmt.Println()
	return result
}

func (cs *ABState) ABPartition_flpa() map[string]uint64 {
	cs.InitConfig()
	if len(cs.NetGraph.EdgeSet) == 0 {
		log.Panic("partition_split.ABPartition:  EdgeSet未累加，为空...")
	}
	result := make(map[string]uint64)
	QueMap := make(map[Vertex]bool, 0)
	updateTreshold := make(map[Vertex]int)
	Que := make([]Vertex, 0)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		//cs.thisTotScore = 0.0
		QueMap = make(map[Vertex]bool, 0)
		Que = make([]Vertex, 0)
		for v := range cs.NetGraph.VertexSet {
			QueMap[v] = true
			Que = append(Que, v)
		}
		head := 0
		tail := len(Que) - 1
		for head < tail { // 遍历所有点
			v := Que[head] // 取元素
			QueMap[v] = false
			head += 1
			updateTreshold[v] += 1
			if updateTreshold[v] > 50 {
				continue
			}
			oldShard := cs.VertexAlloMap[v]
			bestShard, _ := cs.getCorrelationScore(v)
			if oldShard != bestShard && cs.ShardVertexNum[oldShard] > 1 { // 发生改变
				cs.VertexAlloMap[v] = bestShard    // 将v分配到max_scoreShard
				result[v.Addr] = uint64(bestShard) // PartitionMap和res记录的相同的东西
				cs.ShardLoadRecompute(v, oldShard)
				for _, u := range cs.NetGraph.EdgeSet[v] {
					if cs.VertexAlloMap[u] != bestShard {
						if QueMap[u] { // 之前添加过，且还没处理
							continue
						}
						if updateTreshold[u] > 50 {
							continue
						}
						// 之前没被添加到队列
						QueMap[u] = true
						Que = append(Que, u)
						tail += 1
					}
				}
			}
		}
	}

	fmt.Println()
	fmt.Println("********** 划分结果 **********")
	for sid := 0; sid < cs.ShardNum; sid++ {
		fmt.Println("Shard", sid, "vertexNum:", cs.ShardVertexNum[sid],
			"edgeNum", cs.ShardEdgeNum[sid], "= inner", cs.ShardInnerEdgeNum[sid], "+ cross", cs.ShardCrossEdgeNum[sid])
	}
	fmt.Println("跨片边数：", cs.CrossShardEdgeNum)
	fmt.Println()
	return result
}

func (cs *ABState) ABPartition_flpa2() map[string]uint64 {
	cs.InitConfig()
	if len(cs.NetGraph.EdgeSet) == 0 {
		log.Panic("partition_split.ABPartition:  EdgeSet未累加，为空...")
	}
	result := make(map[string]uint64)
	QueMap := make(map[Vertex]bool, 0)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		//cs.thisTotScore = 0.0
		for v := range cs.NetGraph.VertexSet {
			QueMap[v] = true
		}
		for v, _ := range QueMap { // 遍历所有点
			delete(QueMap, v)
			oldShard := cs.VertexAlloMap[v]
			bestShard, _ := cs.getCorrelationScore(v)
			if oldShard != bestShard && cs.ShardVertexNum[oldShard] > 1 {
				cs.VertexAlloMap[v] = bestShard    // 将v分配到max_scoreShard
				result[v.Addr] = uint64(bestShard) // PartitionMap和res记录的相同的东西
				cs.ShardLoadRecompute(v, oldShard)
				for _, u := range cs.NetGraph.EdgeSet[v] {
					_, ok := QueMap[u]
					if cs.VertexAlloMap[u] != bestShard && ok { // 发生改变
						QueMap[u] = true
					}
				}
			}
		}
	}

	fmt.Println()
	fmt.Println("********** 划分结果 **********")
	for sid := 0; sid < cs.ShardNum; sid++ {
		fmt.Println("Shard", sid, "vertexNum:", cs.ShardVertexNum[sid],
			"edgeNum", cs.ShardEdgeNum[sid], "= inner", cs.ShardInnerEdgeNum[sid], "+ cross", cs.ShardCrossEdgeNum[sid])
	}
	fmt.Println("跨片边数：", cs.CrossShardEdgeNum)
	fmt.Println()
	return result
}

func (cs *ABState) GetNewBridge(ABridgesNum, CandidateNum int, GlobalABridges bool, init bool) *core.SingleBridge {
	EdgeSetWithWeight := make(map[Vertex]map[Vertex]int) // 清零
	Vertex_Shard_Weight := make(map[Vertex]map[int]int)
	avgScore := 0.0
	for v, uList := range cs.NetGraph.EdgeSet {
		vShard := cs.VertexAlloMap[v]
		for _, u := range uList {
			if _, ok := EdgeSetWithWeight[v][u]; ok {
				EdgeSetWithWeight[v][u] += 1
			} else { // u不在
				if _, ok := EdgeSetWithWeight[v]; !ok {
					EdgeSetWithWeight[v] = make(map[Vertex]int)
				}
				EdgeSetWithWeight[v][u] = 1
			}

			uShard := cs.VertexAlloMap[u]
			if _, ok := Vertex_Shard_Weight[v][uShard]; ok {
				Vertex_Shard_Weight[v][uShard] += 1
			} else { // u不在
				if _, ok := Vertex_Shard_Weight[v]; !ok {
					Vertex_Shard_Weight[v] = make(map[int]int)
				}
				Vertex_Shard_Weight[v][uShard] = 1
			}

			if vShard == uShard {
				avgScore += 0.5 // 片内交易，累加0.5，uv的时候再算一次，片内交易累加1
			} else {
				avgScore += 1.0
			}
		}
	}
	// 计算平均每个节点在每个分片的平均交易数
	avgScore /= float64(len(cs.NetGraph.EdgeSet) * cs.ShardNum)

	VertexCount := make(map[Vertex]int)
	for _, Vertex_Weight := range EdgeSetWithWeight {
		VertexSet := topCLargestKeys(Vertex_Weight, CandidateNum) // 选出每个分片最热门的 activeAccountSetLen 个账户
		for _, v := range VertexSet {
			if _, ok := VertexCount[v]; !ok {
				VertexCount[v] = 1
			} else {
				VertexCount[v] += 1 // 每个账户被选为一次热门，就累计一次
			}
		}
	}
	ActiveVertexSet := topCLargestKeys(VertexCount, ABridgesNum) // 选择累计次数最多的热门账户

	for i, v := range ActiveVertexSet {
		log.Printf("%d/%d: 交易个数：%d", i, ABridgesNum, len(cs.NetGraph.EdgeSet[v]))
		if i == 5 {
			break
		}
	}

	NewSingleBridge := &core.SingleBridge{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	//BridgeMap := make(map[string]map[uint64]bool, 0)
	if GlobalABridges {
		for _, v := range ActiveVertexSet { // 遍历所有hot账户
			allShardSet := make(map[uint64]bool, 0)
			for i := 0; i < params.ShardNum; i++ {
				allShardSet[uint64(i)] = false
			}
			if init {
				allShardSet[uint64(utils.Addr2Shard(v.Addr))] = true
			}
			NewSingleBridge.AddressQue = append(NewSingleBridge.AddressQue, v.Addr)
			NewSingleBridge.AllocateMap[v.Addr] = allShardSet
		}
		return NewSingleBridge
	}
	for _, v := range ActiveVertexSet {
		alloShardSet := make(map[uint64]bool, 0)
		for shard, weight := range Vertex_Shard_Weight[v] {
			if float64(weight) > avgScore { // 交易数大于平均交易数
				alloShardSet[uint64(shard)] = false
			}
		}
		if init {
			alloShardSet[uint64(utils.Addr2Shard(v.Addr))] = true
		}
		NewSingleBridge.AddressQue = append(NewSingleBridge.AddressQue, v.Addr)
		NewSingleBridge.AllocateMap[v.Addr] = alloShardSet
	}
	return NewSingleBridge
}

func (cs *ABState) GetNewBridgeSimple(ABridgesNum int, init bool) *core.SingleBridge {
	ActiveVertexSet := getTopNLongestEdges(cs.NetGraph.EdgeSet, ABridgesNum)
	for i, v := range ActiveVertexSet {
		log.Printf("%d/%d: 交易个数：%d", i, ABridgesNum, len(cs.NetGraph.EdgeSet[v]))
		if i == 5 {
			break
		}
	}
	NewSingleBridge := &core.SingleBridge{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	for _, v := range ActiveVertexSet { // 遍历所有hot账户
		allShardSet := make(map[uint64]bool, 0)
		for i := 0; i < params.ShardNum; i++ {
			allShardSet[uint64(i)] = false
		}
		if init {
			allShardSet[uint64(utils.Addr2Shard(v.Addr))] = true
		}
		NewSingleBridge.AddressQue = append(NewSingleBridge.AddressQue, v.Addr)
		NewSingleBridge.AllocateMap[v.Addr] = allShardSet
	}
	return NewSingleBridge
}

func (cs *ABState) ABSplit(addNum int, init bool) *core.SingleBridge {
	cs.InitConfig()
	NewBridge := cs.GetNewBridge(addNum, params.CandidateNum, params.GlobalABridges, init)
	//NewBridge := cs.GetNewBridgeSimple(addNum, init)
	/*
		========= 如果存在 2个分片之间不连通 的情况，会报错error tx1 或者 tx2，必须分片间两两互通
	*/
	if len(NewBridge.AllocateMap) == 0 {
		log.Panic("partition_split.ABSplit:  len(NewBridge)==0")
	}
	return NewBridge
}
