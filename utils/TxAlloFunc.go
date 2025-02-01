package utils

//func  getSafeAllo(addr string, PartitionMap map[string]uint64,OldActiveAB,OldIdleAB, NewActiveAB, NewIdleAB *core.SingleBridge) (uint64, []uint64) {
//	_, mainShard, shardList := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
//	if len(PartitionMap) != 0 {
//		mainShard = PartitionMap[addr]
//	}
//	if len(shardList) == 1 {
//		shardList = []uint64{mainShard}
//	}
//	if len(NewActiveAB.AddressQue) != 0 {
//		if _, ok1 := NewActiveAB.AllocateMap[addr]; ok1 {
//			_, slist := GetMapShard(NewActiveAB.AllocateMap[addr])
//			shardList = GetIntersection(shardList, slist)
//		} else if _, ok2 := NewIdleAB.AllocateMap[addr]; ok2 {
//			_, slist := GetMapShard(NewIdleAB.AllocateMap[addr])
//			shardList = GetIntersection(shardList, slist)
//		} else {
//			shardList = []uint64{mainShard}
//		}
//	}
//	return mainShard, shardList
//}

// =============================================================================================
/*
	===========================						=================
*/
//if len(txlist) == 1000 || len(txlist) == 5000 || len(txlist) == 10000 || len(txlist) == 15000 || len(txlist) == 20000 {
//tempDBState := new(partition_split.CLPAState)
//tempDBState.Init_CLPAState(0.5, params.MaxIterations, params.ShardNum) // 新加参数
//
//ccm.sl.Slog.Println("Algo写ABridge...")
//for _, tx := range txlist {
//tempDBState.AddEdge(partition_split.Vertex{Addr: tx.Sender}, partition_split.Vertex{Addr: tx.Recipient})
//}
//SelBridge := tempDBState.ABSplit(100, true)
//filename1 := "./broker/CodeAlgo_" + strconv.Itoa(len(txlist))
//file1, errA := os.Create(filename1)
//if errA != nil {
//fmt.Println("无法创建文件:", err)
//return
//}
//defer file1.Close()
//
//// 创建一个新的Writer
//writer := bufio.NewWriter(file1)
//
//// 遍历字符数组并写入文件
//for _, line := range SelBridge.AddressQue {
//_, err := writer.WriteString(line + "\n")
//if err != nil {
//fmt.Println("写入文件时出错:", err)
//return
//}
//}
//
//// 刷新缓冲区，确保所有数据都被写入文件
//err = writer.Flush()
//if err != nil {
//fmt.Println("刷新缓冲区时出错:", err)
//return
//}
//fmt.Println("文件写入成功")
//}
//
//if len(txlist) == 20000 {
//ccm.sl.Slog.Println("Rand写ABridge...")
//ABridgeAddrMap := make(map[string]bool, 0)
//ABridgeAddr := make([]string, 0)
//for len(ABridgeAddrMap) != 100 {
//rand.Seed(time.Now().UnixNano())
//i := rand.Intn(len(txlist))
//ABridgeAddrMap[txlist[i].Sender] = true
//}
//for key, _ := range ABridgeAddrMap {
//ABridgeAddr = append(ABridgeAddr, key)
//}
//
//filename2 := "./broker/CodeRand"
//file2, errB := os.Create(filename2)
//if errB != nil {
//fmt.Println("无法创建文件:", err)
//return
//}
//defer file2.Close()
//
//// 创建一个新的Writer
//writer2 := bufio.NewWriter(file2)
//
//// 遍历字符数组并写入文件
//for _, line := range ABridgeAddr {
//_, err := writer2.WriteString(line + "\n")
//if err != nil {
//fmt.Println("写入文件时出错:", err)
//return
//}
//}
//// 刷新缓冲区，确保所有数据都被写入文件
//err = writer2.Flush()
//if err != nil {
//fmt.Println("刷新缓冲区时出错:", err)
//return
//}
//fmt.Println("文件写入成功")
//}
//if len(txlist) == 20000 {
//log.Panic("输出结束")
//}
