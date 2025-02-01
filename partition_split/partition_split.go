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

	//ShardVertexListMap map[int][]Vertex // shardID → VertexList shard内有哪些Vertex
	VertexAlloMap     map[Vertex]int // Vertex → shardID
	VertexLoadMap     map[Vertex]int // Vertex → edge num
	ShardCrossEdgeNum []int          // shardID → cross-shard edge num
	ShardInnerEdgeNum []int          // shardID → intra-shard edge num
	ShardEdgeNum      []int          // shardID → edge num
	ShardVertexNum    []int          // shardID → Vertex数

	//EdgeSetWithWeight map[Vertex]map[Vertex]int

	WeightPenalty     float64 // Weight penalty, corresponding to \beta in the paper
	MinShardEdgeNum   int
	AvgShardEdgeNum   float64
	MaxIterations     int // Maximum number of iterations, constraint， Corresponding to the \tau in the paper
	CrossShardEdgeNum int // cross-shard edge num
	ShardNum          int // shard num
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

// To add Vertex, it needs to be assigned to a shard by default
func (cs *ABState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v)
	if val, ok := cs.VertexAlloMap[v]; !ok {
		cs.VertexAlloMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		cs.VertexAlloMap[v] = val
	}
	cs.ShardVertexNum[cs.VertexAlloMap[v]] += 1
}

// To add an edge, its vertex (if not present) needs to be assigned to a shard by default
func (cs *ABState) AddEdge(u, v Vertex) {
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v)
}

func (cs *ABState) InitConfig() {
	// init VertexLoadMap Vertex's 'edge num
	for v, uList := range cs.NetGraph.EdgeSet {
		cs.VertexLoadMap[v] = len(uList)
	}
	//init ShardCrossEdgeNum和ShardInnerEdgeNum cross-shard edge 和intra-shard edge
	cs.ShardCrossEdgeNum = make([]int, cs.ShardNum)
	cs.ShardInnerEdgeNum = make([]int, cs.ShardNum)
	for v, uList := range cs.NetGraph.EdgeSet {
		vShard := cs.VertexAlloMap[v]
		for _, u := range uList {
			uShard := cs.VertexAlloMap[u]
			if vShard != uShard {
				// If Vertex v and u do not belong to the same shard, then add one to the corresponding ShardCrossEdgeNum
				//Only calculate the in degree, so as not to repeat the calculation
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
	//Undirected edge
	// shard load=intra-shard edge+cross-shard edge
	//cs.ShardCrossEdgeNum[ShardID]+cs.ShardInnerEdgeNum[ShardID]
	cs.ShardEdgeNum = make([]int, cs.ShardNum)
	for i := 0; i < cs.ShardNum; i++ {
		cs.ShardInnerEdgeNum[i] /= 2
		cs.ShardEdgeNum[i] = cs.ShardCrossEdgeNum[i] + cs.ShardInnerEdgeNum[i]
	}
}

// 在账户所属shard变动时，重new 计算各个参数，faster
func (cs *ABState) ShardLoadRecompute(v Vertex, oldShardID int) {
	newShardID := cs.VertexAlloMap[v]
	cs.ShardVertexNum[newShardID] += 1
	cs.ShardVertexNum[oldShardID] -= 1
	for _, u := range cs.NetGraph.EdgeSet[v] { // 找到v所有邻接点u
		uShard := cs.VertexAlloMap[u]
		if uShard == oldShardID { // u belong to  old shard
			// old shard：intra-shard edge  → cross-shard edge  intra-shard edge -1 cross-shard edge +1
			cs.ShardInnerEdgeNum[oldShardID] -= 1
			cs.ShardCrossEdgeNum[oldShardID] += 1
			//new shard：无关 edge  → cross-shard edge  num+=1
			cs.ShardCrossEdgeNum[newShardID] += 1
			// 因此cross-shard edge  +=2
			cs.CrossShardEdgeNum += 2
		}
		if uShard == newShardID { // u belong to new shard
			// old shard：cross-shard edge  → 无关 edge  cross-shard edge  -=w
			cs.ShardCrossEdgeNum[oldShardID] -= 1
			//new shard：cross-shard edge  → intra-shard edge  cross-shard edge -1 intra-shard edge +1
			cs.ShardCrossEdgeNum[newShardID] -= 1
			cs.ShardInnerEdgeNum[newShardID] += 1
			// 因此cross-shard edge -=2
			cs.CrossShardEdgeNum -= 2
		}
		if uShard != newShardID && uShard != oldShardID {
			// not belong to new shard，not belong to old shard，no intra-shard edge → cross-shard edge num no change
			// old shard的cross-shard edge  → new shard的cross-shard edge
			cs.ShardCrossEdgeNum[newShardID] += 1
			cs.ShardCrossEdgeNum[oldShardID] -= 1
		}
	}
	for i := 0; i < cs.ShardNum; i++ {
		cs.ShardEdgeNum[i] = cs.ShardCrossEdgeNum[i] + cs.ShardInnerEdgeNum[i]
	}
	cs.MinShardEdgeNum = 0x7ffffffff
	// modify MinShardEdgeNum, CrossShardEdgeNum
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
	v_connNum := len(cs.NetGraph.EdgeSet[v]) //Number of neighbors of v

	for _, u := range cs.NetGraph.EdgeSet[v] {
		uShard := cs.VertexAlloMap[u]
		connToShard[uShard] += 1 // The correlation between v and uShard++
	}
	maxScore := -9999.0
	var maxScoreShard int
	for i := 0; i < cs.ShardNum; i++ {
		shardScore[i] = (float64(connToShard[i]) / float64(v_connNum)) *
			math.Pow(cs.AvgShardEdgeNum/float64(cs.ShardEdgeNum[i]), params.ScorePower)
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
		log.Panic("partition_split.ABPartition:  EdgeSet empty")
	}
	result := make(map[string]uint64)
	for iter := 0; iter < cs.MaxIterations; iter += 1 {
		for v := range cs.NetGraph.VertexSet {
			oldShard := cs.VertexAlloMap[v]
			bestShard, _ := cs.getCorrelationScore(v)
			if oldShard != bestShard && cs.ShardVertexNum[oldShard] > 1 {
				cs.VertexAlloMap[v] = bestShard    // Assign v to max_stcoreShard
				result[v.Addr] = uint64(bestShard) // The same thing recorded by PartitionMap and res
				cs.ShardLoadRecompute(v, oldShard)
			}
		}
	}

	fmt.Println()
	fmt.Println("********** partition solution **********")
	for sid := 0; sid < cs.ShardNum; sid++ {
		fmt.Println("Shard", sid, "vertexNum:", cs.ShardVertexNum[sid],
			"edgeNum", cs.ShardEdgeNum[sid], "= inner", cs.ShardInnerEdgeNum[sid], "+ cross", cs.ShardCrossEdgeNum[sid])
	}
	fmt.Println("cross-shard edge num：", cs.CrossShardEdgeNum)
	fmt.Println()
	return result
}

func (cs *ABState) ABPartition_flpa() map[string]uint64 {
	cs.InitConfig()
	if len(cs.NetGraph.EdgeSet) == 0 {
		log.Panic("partition_split.ABPartition:  EdgeSet empty...")
	}
	result := make(map[string]uint64)
	QueMap := make(map[Vertex]bool, 0)
	updateThreshold := make(map[Vertex]int)
	Que := make([]Vertex, 0)
	for iter := 0; iter < cs.MaxIterations; iter += 1 {
		QueMap = make(map[Vertex]bool)
		Que = make([]Vertex, 0)
		for v := range cs.NetGraph.VertexSet {
			QueMap[v] = true
			Que = append(Que, v)
		}
		head := 0
		tail := len(Que) - 1
		for head < tail {
			v := Que[head]
			QueMap[v] = false
			head += 1
			updateThreshold[v] += 1
			if updateThreshold[v] > 50 {
				continue
			}
			oldShard := cs.VertexAlloMap[v]
			bestShard, _ := cs.getCorrelationScore(v)
			if oldShard != bestShard && cs.ShardVertexNum[oldShard] > 1 {
				cs.VertexAlloMap[v] = bestShard
				result[v.Addr] = uint64(bestShard)
				cs.ShardLoadRecompute(v, oldShard)
				for _, u := range cs.NetGraph.EdgeSet[v] {
					_, ok := QueMap[u]
					if cs.VertexAlloMap[u] != bestShard && ok {
						if QueMap[u] {
							continue
						}
						if updateThreshold[u] > 50 {
							continue
						}
						QueMap[u] = true
						Que = append(Que, u)
						tail += 1
					}
				}
			}
		}
	}

	fmt.Println()
	fmt.Println("********** partition solution **********")
	for sid := 0; sid < cs.ShardNum; sid++ {
		fmt.Println("Shard", sid, "vertexNum:", cs.ShardVertexNum[sid],
			"edgeNum", cs.ShardEdgeNum[sid], "= inner", cs.ShardInnerEdgeNum[sid], "+ cross", cs.ShardCrossEdgeNum[sid])
	}
	fmt.Println("cross-shard edge num：", cs.CrossShardEdgeNum)
	fmt.Println()
	return result
}

func (cs *ABState) GetNewBroker(ABrokersNum, CandidateNum int, GlobalABrokers bool, init bool) *core.SingleBroker {
	EdgeSetWithWeight := make(map[Vertex]map[Vertex]int) // reset
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
				avgScore += 0.5
			} else {
				avgScore += 1.0
			}
		}
	}
	// Calculate the average number of transactions per Vertex on each shard
	avgScore /= float64(len(cs.NetGraph.EdgeSet) * cs.ShardNum)

	VertexCount := make(map[Vertex]int)
	for _, Vertex_Weight := range EdgeSetWithWeight {
		VertexSet := topCLargestKeys(Vertex_Weight, CandidateNum)
		for _, v := range VertexSet {
			if _, ok := VertexCount[v]; !ok {
				VertexCount[v] = 1
			} else {
				VertexCount[v] += 1
			}
		}
	}
	ActiveVertexSet := topCLargestKeys(VertexCount, ABrokersNum) // Select the popular account with the highest cumulative number of times

	for i, v := range ActiveVertexSet {
		log.Printf("%d/%d: TX num：%d", i, ABrokersNum, len(cs.NetGraph.EdgeSet[v]))
		if i == 5 {
			break
		}
	}

	NewSingleBroker := &core.SingleBroker{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	//BrokerMap := make(map[string]map[uint64]bool, 0)
	if GlobalABrokers {
		for _, v := range ActiveVertexSet { // Traverse all active  accounts
			allShardSet := make(map[uint64]bool, 0)
			for i := 0; i < params.ShardNum; i++ {
				allShardSet[uint64(i)] = false
			}
			if init {
				allShardSet[uint64(utils.Addr2Shard(v.Addr))] = true
			}
			NewSingleBroker.AddressQue = append(NewSingleBroker.AddressQue, v.Addr)
			NewSingleBroker.AllocateMap[v.Addr] = allShardSet
		}
		return NewSingleBroker
	}
	for _, v := range ActiveVertexSet {
		alloShardSet := make(map[uint64]bool, 0)
		for shard, weight := range Vertex_Shard_Weight[v] {
			if float64(weight) > avgScore {
				alloShardSet[uint64(shard)] = false
			}
		}
		if init {
			alloShardSet[uint64(utils.Addr2Shard(v.Addr))] = true
		}
		NewSingleBroker.AddressQue = append(NewSingleBroker.AddressQue, v.Addr)
		NewSingleBroker.AllocateMap[v.Addr] = alloShardSet
	}
	return NewSingleBroker
}

func (cs *ABState) GetNewBrokerSimple(ABrokersNum int, init bool) *core.SingleBroker {
	ActiveVertexSet := getTopNLongestEdges(cs.NetGraph.EdgeSet, ABrokersNum)
	for i, v := range ActiveVertexSet {
		log.Printf("%d/%d: 交易个数：%d", i, ABrokersNum, len(cs.NetGraph.EdgeSet[v]))
		if i == 5 {
			break
		}
	}
	NewSingleBroker := &core.SingleBroker{
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
		NewSingleBroker.AddressQue = append(NewSingleBroker.AddressQue, v.Addr)
		NewSingleBroker.AllocateMap[v.Addr] = allShardSet
	}
	return NewSingleBroker
}

func (cs *ABState) GetNewBrokerAdapt(ABrokersNum int, init bool) *core.SingleBroker {
	crossEdgeSet := make(map[Vertex][]Vertex)

	for v, uList := range cs.NetGraph.EdgeSet {
		for _, u := range uList {
			if cs.VertexAlloMap[v] != cs.VertexAlloMap[u] { // 同一个shard
				crossEdgeSet[v] = append(crossEdgeSet[v], u)
			}
		}
	}
	ActiveVertexSet := getTopNLongestEdges(crossEdgeSet, ABrokersNum)
	for i, v := range ActiveVertexSet {
		log.Printf("%d/%d: TX num：%d, cross-shard TX num：%d", i, ABrokersNum,
			len(cs.NetGraph.EdgeSet[v]), len(crossEdgeSet[v]))
		if i == 5 {
			break
		}
	}
	NewSingleBroker := &core.SingleBroker{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	for _, v := range ActiveVertexSet {
		allShardSet := make(map[uint64]bool, 0)
		for i := 0; i < params.ShardNum; i++ {
			allShardSet[uint64(i)] = false
		}
		if init {
			allShardSet[uint64(utils.Addr2Shard(v.Addr))] = true
		}
		NewSingleBroker.AddressQue = append(NewSingleBroker.AddressQue, v.Addr)
		NewSingleBroker.AllocateMap[v.Addr] = allShardSet
	}
	return NewSingleBroker
}

func (cs *ABState) ABSplit(addNum int, init bool) *core.SingleBroker {
	cs.InitConfig()
	NewBroker := cs.GetNewBroker(addNum, params.CandidateNum, params.GlobalABrokers, init)
	//NewBroker := cs.GetNewBrokerSimple(addNum, init)
	//NewBroker := cs.GetNewBrokerAdapt(addNum, init)
	if len(NewBroker.AllocateMap) == 0 {
		log.Panic("partition_split.ABSplit:  len(NewBroker)==0")
	}
	return NewBroker
}
