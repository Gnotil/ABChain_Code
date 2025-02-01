package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/json"
	"log"
	"math/big"
	"math/rand"
	"time"
)

/**
==================================== split ********************************************
*/

func (pihm *PBFTInsideHandleModule) sendMixUpdateReady() {
	pihm.cdm.ReadyLock.Lock()
	pihm.cdm.MixUpdateReadyMap[pihm.pbftNode.ShardID] = true // 当前分片的split设置为ready
	pihm.cdm.ReadyLock.Unlock()

	pr := message.MixUpdateReady{
		FromShard: pihm.pbftNode.ShardID,
		NowSeqID:  pihm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.C2C_MixUpdateReady, pByte)
	for sid := 0; sid < int(pihm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			networks.TcpDial(send_msg, pihm.pbftNode.ip_nodeTable[uint64(sid)][0]) // 告诉其他所有分片，自己准备好split了
		}
	}
	pihm.pbftNode.pl.Plog.Print("Ready for MixUpdate\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (pihm *PBFTInsideHandleModule) getMixUpdateReady() bool {
	pihm.cdm.ReadyLock.Lock()
	defer pihm.cdm.ReadyLock.Unlock()
	pihm.pbftNode.seqMapLock.Lock()
	defer pihm.pbftNode.seqMapLock.Unlock()
	pihm.cdm.ReadySeqLock.Lock()
	defer pihm.cdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range pihm.pbftNode.seqIDMap {
		if rval, ok := pihm.cdm.ReadySeqMap[sid]; !ok || (rval-1 != val) {
			flag = false
		}
		if rval, ok := pihm.cdm.ReadySeqMap[sid]; ok && (rval-1 == val) {
			if flag == false {
				flag = true
			}
		}
	}
	// PartitionReady == 分片数
	return len(pihm.cdm.MixUpdateReadyMap) == int(pihm.pbftNode.pbftChainConfig.ShardNums) && flag // 2个条件都要满足
}

func (pihm *PBFTInsideHandleModule) MixUpdateMigrateAccount(PartitionMap map[string]uint64, NewBridge, DelBridge *core.SingleBridge) {
	/*
		=============================== PartitionMap
	*/
	for addr, toShard := range PartitionMap {
		_, addrMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
		_, addrShardList := pihm.pbftNode.CurChain.ABridges.GetBridgeShardList(addr)
		if len(addrShardList) == 0 {
			addrShardList = []uint64{addrMainShard}
		}
		_, ok1 := NewBridge.AllocateMap[addr]
		_, ok2 := DelBridge.AllocateMap[addr]
		if ok1 || ok2 { // 新ABridge或者expired Abridge，跳过
			//后面再处理
			continue
		}
		if len(addrShardList) > 1 { //idle ABridge
			//后面更新状态的时候，改一下mainShard就行
			continue
		}
		if addrMainShard == pihm.pbftNode.ShardID && toShard != pihm.pbftNode.ShardID { // 存储在当前分片，但目标分片不是当前分片，说明当前账户时需要迁移走的 to shard
			pihm.migSup.AddrSend[toShard] = append(pihm.migSup.AddrSend[toShard], addr)
			pihm.pbftNode.CurChain.Aslock.Lock()
			if _, ok := pihm.pbftNode.CurChain.AccountState[addr]; ok {
				state := core.CopyAccountState(pihm.pbftNode.CurChain.AccountState[addr])
				pihm.migSup.StateSend[toShard] = append(pihm.migSup.StateSend[toShard], state)
			} else {
				//pihm.pbftNode.pl.Plog.Println("addr存储在当前分片，但是state为空")
				//pihm.pbftNode.pl.Plog.Println(isExist, addrMainShard, addrShardList, "to:", toShard)
				state := core.ConstructAccount(addr, false)
				pihm.pbftNode.CurChain.AccountState[addr] = core.CopyAccountState(state)
				pihm.migSup.StateSend[toShard] = append(pihm.migSup.StateSend[toShard], state)
			}
			delete(pihm.pbftNode.CurChain.AccountState, addr)
			pihm.pbftNode.CurChain.Aslock.Unlock()
		}
	}
	/*
		=============================== NewBridge
	*/
	for addr, toShardList := range NewBridge.AllocateMap { // 遍历ABridge列表
		if len(toShardList) <= 1 {
			pihm.pbftNode.pl.Plog.Println("Bridge账户分布<=1")
			log.Panic()
		}
		_, addrMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
		_, addrShardList := pihm.pbftNode.CurChain.ABridges.GetBridgeShardList(addr)
		if len(addrShardList) == 0 {
			addrShardList = []uint64{addrMainShard}
		}
		if utils.InIntList(pihm.pbftNode.ShardID, addrShardList) {
			if _, ok := pihm.pbftNode.CurChain.AccountState[addr]; !ok { // 账户addr早就被分布到当前分片，但是没有给它创建状态
				//pihm.pbftNode.pl.Plog.Println("账户addr早就被分布到当前分片，", isExist, addrMainShard, addrShardList, "但是没有给它创建状态")
				account := core.ConstructAccount(addr, false)
				shardNum := big.NewInt(int64(len(addrShardList)))
				value := big.NewInt(int64(0))
				rem := big.NewInt(int64(0))
				value.QuoRem(account.Balance, shardNum, rem)
				_, allo := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
				if allo == pihm.pbftNode.ShardID {
					account.Balance.Add(value, rem)
				}
				if len(addrShardList) > 1 {
					account.IsSubAccount = true
				}
				pihm.pbftNode.CurChain.Aslock.Lock()
				pihm.pbftNode.CurChain.AccountState[addr] = account // 没状态，创建状态
				pihm.pbftNode.CurChain.Aslock.Unlock()
			}
		} else { // 当前账户addr不存在于本分片
			continue
		}

		addrState := core.CopyAccountState(pihm.pbftNode.CurChain.AccountState[addr]) // 账户addr的分身状态

		for sd, st := range addrState.SplitAccount(toShardList) {
			if sd == pihm.pbftNode.ShardID {
				pihm.pbftNode.CurChain.Aslock.Lock()
				pihm.pbftNode.CurChain.AccountState[st.Address] = st // 当前分片留一份
				pihm.pbftNode.CurChain.Aslock.Unlock()
			} else {
				pihm.migSup.AddrSend[sd] = append(pihm.migSup.AddrSend[sd], addr)
				pihm.migSup.StateSend[sd] = append(pihm.migSup.StateSend[sd], st) // 发给目标分片
			}
		}
		delete(pihm.pbftNode.CurChain.AccountState, addr) // 因为需要迁移出去，所以从本分片删除
	}
	/*
		=============================== DelBridge
	*/
	for _, addr := range DelBridge.AddressQue {
		isExist, addrMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
		_, addrShardList := pihm.pbftNode.CurChain.ABridges.GetBridgeShardList(addr)
		if len(addrShardList) == 0 {
			addrShardList = []uint64{addrMainShard}
		}
		if _, ok := PartitionMap[addr]; ok {
			addrMainShard = PartitionMap[addr] // 这个失效的ABridge应该把它的账户信息发到这里
		}

		if len(DelBridge.AllocateMap[addr]) <= 1 {
			pihm.pbftNode.pl.Plog.Println("Bridge分布错误: ", isExist, addrMainShard, addrShardList, " -- ", len(DelBridge.AllocateMap[addr]))
			log.Panic()
		}
		isBridge := pihm.pbftNode.CurChain.ABridges.IsBridge(addr)
		if !utils.InIntList(pihm.pbftNode.ShardID, addrShardList) {
			pihm.pbftNode.pl.Plog.Println("当前分片不在addr的分布: ", isExist, addrMainShard, addrShardList, "    bridgeSign：", isBridge)
			continue
		}
		if _, ok := pihm.pbftNode.CurChain.AccountState[addr]; !ok {
			//pihm.pbftNode.pl.Plog.Println("账户addr早就被分布到当前分片，", isExist, addrMainShard, addrShardList, "但是没有给它创建状态")
			account := core.ConstructAccount(addr, false)
			shardNum := big.NewInt(int64(len(addrShardList)))
			value := big.NewInt(int64(0))
			rem := big.NewInt(int64(0))
			value.QuoRem(account.Balance, shardNum, rem)
			_, allo := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
			if allo == pihm.pbftNode.ShardID {
				account.Balance.Add(value, rem)
			}
			account.IsSubAccount = true
			pihm.pbftNode.CurChain.Aslock.Lock()
			pihm.pbftNode.CurChain.AccountState[addr] = account // 没状态，创建状态
			pihm.pbftNode.CurChain.Aslock.Unlock()
		}
		if addrMainShard == pihm.pbftNode.ShardID { // 从其他分片将状态发送到addrMainShard
			continue
		}
		addrState := core.CopyAccountState(pihm.pbftNode.CurChain.AccountState[addr])                  // 账户addr在当前分片的分身状态
		pihm.migSup.StateSend[addrMainShard] = append(pihm.migSup.StateSend[addrMainShard], addrState) // 发给主状态分片
		delete(pihm.pbftNode.CurChain.AccountState, addr)
	}
}

func (pihm *PBFTInsideHandleModule) MixUpdateMigrateTx(PartitionMap map[string]uint64, NewActiveBridge, NewIdleBridge *core.SingleBridge) {
	firstPtr := 0
	pihm.pbftNode.pl.Plog.Println("当前交易池大小：", len(pihm.pbftNode.CurChain.Txpool.TxQueue))
	for secondPtr := 0; secondPtr < len(pihm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
		ptx := pihm.pbftNode.CurChain.Txpool.TxQueue[secondPtr] // 从队列里面取？
		// whether should be transfer or not

		//senderMainShard, senderShardList := pihm.getActiveAllo(ptx.Sender, PartitionMap, NewActiveBridge, NewIdleBridge)          //1230
		//recipientMainShard, recipientShardList := pihm.getActiveAllo(ptx.Recipient, PartitionMap, NewActiveBridge, NewIdleBridge) //0
		//if ptx.RawTxHash != nil {
		//	senderMainShard, senderShardList = pihm.getNewAllo(ptx.Sender, PartitionMap, NewActiveBridge, NewIdleBridge)          //1230
		//	recipientMainShard, recipientShardList = pihm.getNewAllo(ptx.Recipient, PartitionMap, NewActiveBridge, NewIdleBridge) //0
		senderMainShard, senderShardList := pihm.getNewAllo(ptx.Sender, PartitionMap, NewActiveBridge, NewIdleBridge)          //1230
		recipientMainShard, recipientShardList := pihm.getNewAllo(ptx.Recipient, PartitionMap, NewActiveBridge, NewIdleBridge) //0

		removeFlag := false

		IntersecList := utils.GetIntersection(senderShardList, recipientShardList) // 0
		if len(IntersecList) == 0 {
			pihm.migSup.TxsCross = append(pihm.migSup.TxsCross, ptx)
			removeFlag = true
			if ptx.RawTxHash != nil {
				pihm.pbftNode.pl.Plog.Println("拆分过的交易，还需要转发给主分片：")
			}
			//pihm.pbftNode.pl.Plog.Println("senderShardList：", senderShardList)
			//pihm.pbftNode.pl.Plog.Println("recipientShardList：", recipientShardList)
			//pihm.pbftNode.pl.Plog.Println("需要转发给主分片：")
		} else {
			if !utils.InIntList(pihm.pbftNode.ShardID, IntersecList) { // 当前分片不在分片交集里，则需要迁移 2
				removeFlag = true
				rand.Seed(time.Now().UnixNano())
				r := rand.Intn(len(IntersecList))
				toShard := IntersecList[r]
				if len(IntersecList) > 1 {
					if core.NewestThan(ptx.Sender, ptx.Recipient, NewActiveBridge, NewIdleBridge) == 1 { //Sender更加新，应该发给Recipient
						if utils.InIntList(recipientMainShard, IntersecList) {
							toShard = recipientMainShard
						}
					} else {
						if utils.InIntList(senderMainShard, IntersecList) {
							toShard = senderMainShard
						}
					}
				}
				pihm.migSup.TxsSend[toShard] = append(pihm.migSup.TxsSend[toShard], ptx)
				//pihm.pbftNode.pl.Plog.Printf("%d(%t) → %d(%t) = S%d", senderShardList, ptx.OriginalSender == ptx.Sender, recipientShardList,
				//	ptx.FinalRecipient == ptx.Recipient, toShard)
			}
		}

		if !removeFlag { // removeFlag=false时，条件为真，此时队列头前移，该交易会被保存到目前的txpool中
			pihm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = pihm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			firstPtr++ // 指针++就代表跳过
		}
	}
	pihm.pbftNode.CurChain.Txpool.TxQueue = pihm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr] //从队列里面 出队
	//pihm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", shardIdx)
	pihm.pbftNode.pl.Plog.Println("交易出队后，交易池大小：", len(pihm.pbftNode.CurChain.Txpool.TxQueue))
}

func (pihm *PBFTInsideHandleModule) MixUpdateMigrateMain() {
	pihm.pbftNode.pl.Plog.Printf("开始迁移账户状态: MigrateNewMain, MixUpdateCode: %d", pihm.cdm.MixUpdateCode)
	lastBridgeMapId := len(pihm.cdm.NewBridgeMap) - 1
	lastPartitionMapId := len(pihm.cdm.PartitionMap) - 1
	NowABridge := pihm.pbftNode.CurChain.Get_BridgeMap()
	NowABridge.CheckBridge()

	NewActiveBridge, NewIdleBridge, deleteBridge := NowABridge.PreUpdateBridgeMap(pihm.cdm.NewBridgeMap[lastBridgeMapId])

	//for _, i := range NewActiveBridge.AddressQue {
	//	pihm.pbftNode.pl.Plog.Println(i)
	//}
	//pihm.pbftNode.pl.Plog.Println("=======================================")
	//for _, i := range NewIdleBridge.AddressQue {
	//	pihm.pbftNode.pl.Plog.Println(i)
	//}

	pihm.cdm.DeleteBridgeQue = append(pihm.cdm.DeleteBridgeQue, deleteBridge)
	pihm.pbftNode.CurChain.Txpool.GetLocked()

	pihm.MixUpdateMigrateAccount(pihm.cdm.PartitionMap[lastPartitionMapId], pihm.cdm.NewBridgeMap[lastBridgeMapId], pihm.cdm.DeleteBridgeQue[lastBridgeMapId])
	pihm.MixUpdateMigrateTx(pihm.cdm.PartitionMap[lastPartitionMapId], NewActiveBridge, NewIdleBridge)
	pihm.pbftNode.CurChain.Txpool.GetUnlocked()

	for shardIdx := uint64(0); shardIdx < pihm.pbftNode.pbftChainConfig.ShardNums; shardIdx++ {
		if shardIdx == pihm.pbftNode.ShardID { //不需要自己给自己发
			continue
		}
		ast := message.AccountStateAndTx{
			Addrs:        pihm.migSup.AddrSend[shardIdx],
			AccountState: pihm.migSup.StateSend[shardIdx],
			Txs:          pihm.migSup.TxsSend[shardIdx],
			FromShard:    pihm.pbftNode.ShardID, //from 当前分片
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.C2C_MixUpdateTxMsg, aByte)
		networks.TcpDial(send_msg, pihm.pbftNode.ip_nodeTable[shardIdx][0]) //所有的片内交易 发给分片i
		pihm.pbftNode.pl.Plog.Printf("The message to shard %d is sent，账户数：%d，状态数：%d，交易数：%d\n",
			shardIdx, len(ast.Addrs), len(ast.AccountState), len(ast.Txs))
	}

	// send these txs to supervisor
	if len(pihm.migSup.TxsCross) != 0 {
		i2ctx := message.InnerTx2CrossTx{
			FromShard: pihm.pbftNode.ShardID,
			Txs:       pihm.migSup.TxsCross,
		}

		icByte, err := json.Marshal(i2ctx)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.CrInner2CrossTx, icByte)
		networks.TcpDial(send_msg, pihm.pbftNode.ip_nodeTable[params.DeciderShard][0]) // 跨片交易。发给supervisor
		pihm.pbftNode.pl.Plog.Printf("The message to DeciderShard is sent，交易数：%d\n", len(i2ctx.Txs))
	}

	pihm.migSup.ResetMigrateSupport() // 重置
}

// fetch collect infos
func (pihm *PBFTInsideHandleModule) getMixUpdateCollectOver() bool {
	pihm.cdm.CollectLock.Lock()
	defer pihm.cdm.CollectLock.Unlock()
	return pihm.cdm.CollectOver
}

// propose a split message
func (pihm *PBFTInsideHandleModule) proposeMixUpdate() (bool, *message.Request) {
	pihm.pbftNode.pl.Plog.Printf("S%dN%d : begin MixUpdate proposing\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
	// add all data in pool into the set
	for s, at := range pihm.cdm.AccountStateTx { // AccountMixUpdateStateTx：其他分片发来的交易 -- 遍历分片
		for i, state := range at.AccountState { // 遍历某个分片发来的状态
			addr := state.Address
			if _, ok := pihm.cdm.DeleteBridgeQue[len(pihm.cdm.DeleteBridgeQue)-1].AllocateMap[addr]; ok {
				pihm.pbftNode.pl.Plog.Printf("收到分片%d发来的删除分片状态", s)
			}
			if _, ok := pihm.cdm.ReceivedNewAccountState[addr]; !ok { // 新来的
				pihm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
			} else {
				//把新遇到的account分身碎片state，把新碎片加到已有的account分身
				pihm.cdm.ReceivedNewAccountState[addr].AggregateAccount(at.AccountState[i])
			}
		}
		pihm.cdm.ReceivedNewTx = append(pihm.cdm.ReceivedNewTx, at.Txs...)
	}
	pihm.pbftNode.pl.Plog.Printf("S%dN%d : 收到%d个账户和%d个交易\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, len(pihm.cdm.ReceivedNewAccountState), len(pihm.cdm.ReceivedNewTx))
	// propose, send all txs to other nodes in shard
	pihm.pbftNode.CurChain.Txpool.AddTxs2Pool(pihm.cdm.ReceivedNewTx) // Txpool添加交易

	pihm.pbftNode.pl.Plog.Println("从其他分片收到交易后，交易池大小：", len(pihm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range pihm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
		pihm.pbftNode.CurChain.Aslock.Lock()
		pihm.pbftNode.CurChain.AccountState[key] = val //把所有收到的状态统计
		pihm.pbftNode.CurChain.Aslock.Unlock()
	}

	atm := message.AccountMixUpdateMsg{
		SingleBridge: pihm.cdm.NewBridgeMap[pihm.cdm.AccountTransferRound],
		PartitionMap: pihm.cdm.PartitionMap[pihm.cdm.AccountTransferRound],
		Addrs:        atmaddr,
		AccountState: atmAs,
		ATid:         uint64(len(pihm.cdm.NewBridgeMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.MixUpdateReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}
	return true, r
}

// all nodes in a shard will do accout MixUpdate, to sync the state trie
func (pihm *PBFTInsideHandleModule) accountMixUpdate_do(asm *message.AccountMixUpdateMsg) {
	pihm.pbftNode.pl.Plog.Println("更新Bridge和AllocationMap")
	if len(asm.PartitionMap) != 0 {
		pihm.pbftNode.CurChain.Update_AllocationMap(asm.PartitionMap)
		pihm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", len(asm.PartitionMap))
	}

	if len(asm.SingleBridge.AddressQue) != 0 {
		delBridge := pihm.pbftNode.CurChain.Update_BridgeMap(asm.SingleBridge)
		pihm.pbftNode.pl.Plog.Printf("当前分片S%dN%d需要移除%d个分身账户", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, len(delBridge.AddressQue))
	}

	pihm.pbftNode.CurChain.AddAccounts(asm.AccountState) // 添加分身账户

	//pihm.pbftNode.pl.Plog.Println("++++++++++++++++++++++++++++++++++++++ActiveBridge")
	//for _, i := range pihm.pbftNode.CurChain.Get_BridgeMap().ActiveBridge.AddressQue {
	//	pihm.pbftNode.pl.Plog.Println(i)
	//}
	//pihm.pbftNode.pl.Plog.Println("++++++++++++++++++++++++++++++++++++++IdleBridge")
	//for _, i := range pihm.pbftNode.CurChain.Get_BridgeMap().IdleBridge.AddressQue {
	//	pihm.pbftNode.pl.Plog.Println(i)
	//}

	if uint64(len(pihm.cdm.NewBridgeMap)) != asm.ATid {
		pihm.cdm.NewBridgeMap = append(pihm.cdm.NewBridgeMap, asm.SingleBridge)
	}

	if uint64(len(pihm.cdm.PartitionMap)) != asm.ATid {
		pihm.cdm.PartitionMap = append(pihm.cdm.PartitionMap, asm.PartitionMap)
	}

	pihm.cdm.AccountTransferRound = asm.ATid
	pihm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	pihm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	pihm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	pihm.cdm.MixUpdateCode = 0

	pihm.cdm.CollectLock.Lock()
	pihm.cdm.CollectOver = false
	pihm.cdm.CollectLock.Unlock()

	pihm.cdm.ReadyLock.Lock()
	pihm.cdm.MixUpdateReadyMap = make(map[uint64]bool)
	pihm.cdm.ReadyLock.Unlock()

	pihm.pbftNode.CurChain.PrintBlockChain()

	if pihm.pbftNode.NodeID == pihm.pbftNode.view { //只让主节点发送
		// 发送所有 许可的bridge 给A-Shard
		PermitInfoSet := make([]message.MixUpdateInfoStruct, 0)
		for idx, addr := range asm.SingleBridge.AddressQue { // 直接遍历
			//pihm.pbftNode.pl.Plog.Println(addr)
			shard := asm.SingleBridge.AllocateMap[addr]
			if utils.InMapKeys(pihm.pbftNode.ShardID, shard) { // 分布到当前分片
				PermitInfo := message.MixUpdateInfoStruct{
					SenderAddress: addr,
					SenderShardID: pihm.pbftNode.ShardID,
					AssoShard:     shard,
					Index:         idx,
				}
				PermitInfoSet = append(PermitInfoSet, PermitInfo)
			}
		}
		if len(PermitInfoSet) != 0 {
			pim := message.MixUpdateInfoMsg{
				FromShard:     pihm.pbftNode.ShardID,
				MixUpdateInfo: PermitInfoSet,
			}
			pimByte, err := json.Marshal(pim)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.C2A_MixUpdateReplyMsg, pimByte)
			networks.TcpDial(msg_send, pihm.pbftNode.ip_nodeTable[params.DeciderShard][0])
			pihm.pbftNode.pl.Plog.Printf("S%dN%d : account num=%d 发送MixUpdate许可 ids to A-Shard\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, len(PermitInfoSet))
		}
	}
}
