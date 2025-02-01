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

func (pihm *PBFTInsideHandleModule) sendMixUpdateReady() {
	pihm.cdm.ReadyLock.Lock()
	pihm.cdm.MixUpdateReadyMap[pihm.pbftNode.ShardID] = true // set to Ready
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
			networks.TcpDial(send_msg, pihm.pbftNode.ip_nodeTable[uint64(sid)][0]) // Tell all the other shards that this shard is ready to split
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
	return len(pihm.cdm.MixUpdateReadyMap) == int(pihm.pbftNode.pbftChainConfig.ShardNums) && flag
}

func (pihm *PBFTInsideHandleModule) MixUpdateMigrateAccount(PartitionMap map[string]uint64, NewBroker, DelBroker *core.SingleBroker) {
	/*
		=============================== PartitionMap
	*/
	for addr, toShard := range PartitionMap {
		_, addrMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
		_, addrShardList := pihm.pbftNode.CurChain.ABrokers.GetBrokerShardList(addr)
		if len(addrShardList) == 0 {
			addrShardList = []uint64{addrMainShard}
		}
		_, ok1 := NewBroker.AllocateMap[addr]
		_, ok2 := DelBroker.AllocateMap[addr]
		if ok1 || ok2 { //If it is a new ABroker or expired Abridge, skip it
			continue
		}
		if len(addrShardList) > 1 { //idle ABroker
			//When you update the status later, just change mainShard
			continue
		}
		if addrMainShard == pihm.pbftNode.ShardID && toShard != pihm.pbftNode.ShardID { // If the storage is stored in the current shard, but the target shard is not the current shard, the current account needs to be migrated to shard
			pihm.migSup.AddrSend[toShard] = append(pihm.migSup.AddrSend[toShard], addr)
			pihm.pbftNode.CurChain.Aslock.Lock()
			if _, ok := pihm.pbftNode.CurChain.AccountState[addr]; ok {
				state := core.CopyAccountState(pihm.pbftNode.CurChain.AccountState[addr])
				pihm.migSup.StateSend[toShard] = append(pihm.migSup.StateSend[toShard], state)
			} else {
				state := core.ConstructAccount(addr, false)
				pihm.pbftNode.CurChain.AccountState[addr] = core.CopyAccountState(state)
				pihm.migSup.StateSend[toShard] = append(pihm.migSup.StateSend[toShard], state)
			}
			delete(pihm.pbftNode.CurChain.AccountState, addr)
			pihm.pbftNode.CurChain.Aslock.Unlock()
		}
	}
	/*
		=============================== NewBroker
	*/
	for addr, toShardList := range NewBroker.AllocateMap { // 遍历ABroker列表
		if len(toShardList) <= 1 {
			log.Panic()
		}
		_, addrMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
		_, addrShardList := pihm.pbftNode.CurChain.ABrokers.GetBrokerShardList(addr)
		if len(addrShardList) == 0 {
			addrShardList = []uint64{addrMainShard}
		}
		if utils.InIntList(pihm.pbftNode.ShardID, addrShardList) {
			if _, ok := pihm.pbftNode.CurChain.AccountState[addr]; !ok { // The account addr has already been distributed to the current shard, but no state has been created for it
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
				pihm.pbftNode.CurChain.AccountState[addr] = account // No status, create status
				pihm.pbftNode.CurChain.Aslock.Unlock()
			}
		} else { // The current account addr does not exist in this shard
			continue
		}

		addrState := core.CopyAccountState(pihm.pbftNode.CurChain.AccountState[addr]) // split-status of the account addr

		for sd, st := range addrState.SplitAccount(toShardList) {
			if sd == pihm.pbftNode.ShardID {
				pihm.pbftNode.CurChain.Aslock.Lock()
				pihm.pbftNode.CurChain.AccountState[st.Address] = st // Keep a split-state of the current shard
				pihm.pbftNode.CurChain.Aslock.Unlock()
			} else {
				pihm.migSup.AddrSend[sd] = append(pihm.migSup.AddrSend[sd], addr)
				pihm.migSup.StateSend[sd] = append(pihm.migSup.StateSend[sd], st) // Send to destination shards
			}
		}
		delete(pihm.pbftNode.CurChain.AccountState, addr) //Because accounts need to be migrated out, these accounts need to be deleted from this shard
	}
	/*
		=============================== DelBroker
	*/
	for _, addr := range DelBroker.AddressQue {
		isExist, addrMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(addr)
		_, addrShardList := pihm.pbftNode.CurChain.ABrokers.GetBrokerShardList(addr)
		if len(addrShardList) == 0 {
			addrShardList = []uint64{addrMainShard}
		}
		if _, ok := PartitionMap[addr]; ok {
			addrMainShard = PartitionMap[addr] // The expired ABroker should send its account state here
		}

		if len(DelBroker.AllocateMap[addr]) <= 1 {
			pihm.pbftNode.pl.Plog.Println("broker partiton to a single shard: ", isExist, addrMainShard, addrShardList, " -- ", len(DelBroker.AllocateMap[addr]))
			log.Panic()
		}
		isBroker := pihm.pbftNode.CurChain.ABrokers.IsBroker(addr)
		if !utils.InIntList(pihm.pbftNode.ShardID, addrShardList) {
			pihm.pbftNode.pl.Plog.Println("The current fragment is not in the associated shard of the addr: ", isExist, addrMainShard, addrShardList, "    bridgeSign：", isBroker)
			continue
		}
		if _, ok := pihm.pbftNode.CurChain.AccountState[addr]; !ok {
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
			pihm.pbftNode.CurChain.AccountState[addr] = account
			pihm.pbftNode.CurChain.Aslock.Unlock()
		}
		if addrMainShard == pihm.pbftNode.ShardID {
			continue
		}
		addrState := core.CopyAccountState(pihm.pbftNode.CurChain.AccountState[addr])                  // The split-status of the addr account in the current shard
		pihm.migSup.StateSend[addrMainShard] = append(pihm.migSup.StateSend[addrMainShard], addrState) // Sent to main shard
		delete(pihm.pbftNode.CurChain.AccountState, addr)
	}
}

func (pihm *PBFTInsideHandleModule) MixUpdateMigrateTx(PartitionMap map[string]uint64, NewActiveBroker, NewIdleBroker *core.SingleBroker) {
	firstPtr := 0
	pihm.pbftNode.pl.Plog.Println("Size of the current transaction pool：", len(pihm.pbftNode.CurChain.Txpool.TxQueue))
	for secondPtr := 0; secondPtr < len(pihm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
		ptx := pihm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
		// whether should be transfer or not
		senderMainShard, senderShardList := pihm.getNewAllo(ptx.Sender, PartitionMap, NewActiveBroker, NewIdleBroker)          //1230
		recipientMainShard, recipientShardList := pihm.getNewAllo(ptx.Recipient, PartitionMap, NewActiveBroker, NewIdleBroker) //0

		removeFlag := false

		IntersecList := utils.GetIntersection(senderShardList, recipientShardList) // 0
		if len(IntersecList) == 0 {
			pihm.migSup.TxsCross = append(pihm.migSup.TxsCross, ptx)
			removeFlag = true
			if ptx.RawTxHash != nil {
				pihm.pbftNode.pl.Plog.Println("Split transaction, needs to be forwarded to the main shard")
			}
		} else {
			if !utils.InIntList(pihm.pbftNode.ShardID, IntersecList) { // If the current shard is not in the shard intersection, migration is required
				removeFlag = true
				rand.Seed(time.Now().UnixNano())
				r := rand.Intn(len(IntersecList))
				toShard := IntersecList[r]
				if len(IntersecList) > 1 {
					if core.NewestThan(ptx.Sender, ptx.Recipient, NewActiveBroker, NewIdleBroker) == 1 { //The Sender has a longer lifecycle and should be sent to the Recipient
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
			}
		}

		if !removeFlag { //If removeFlag=false and the condition is true, the queue header moves forward and the transaction is saved to the current txpool
			pihm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = pihm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			firstPtr++ // The pointer ++ means skip
		}
	}
	pihm.pbftNode.CurChain.Txpool.TxQueue = pihm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr] //Dequeue
	pihm.pbftNode.pl.Plog.Println("Size of the transaction pool after the transaction is dequeue：", len(pihm.pbftNode.CurChain.Txpool.TxQueue))
}

func (pihm *PBFTInsideHandleModule) MixUpdateMigrateMain() {
	pihm.pbftNode.pl.Plog.Printf("开始迁移账户状态: MigrateNewMain, MixUpdateCode: %d", pihm.cdm.MixUpdateCode)
	lastBrokerMapId := len(pihm.cdm.NewBrokerMap) - 1
	lastPartitionMapId := len(pihm.cdm.PartitionMap) - 1
	NowABroker := pihm.pbftNode.CurChain.Get_BrokerMap()
	NowABroker.CheckBroker()

	NewActiveBroker, NewIdleBroker, deleteBroker := NowABroker.PreUpdateBrokerMap(pihm.cdm.NewBrokerMap[lastBrokerMapId])

	pihm.cdm.DeleteBrokerQue = append(pihm.cdm.DeleteBrokerQue, deleteBroker)
	pihm.pbftNode.CurChain.Txpool.GetLocked()

	pihm.MixUpdateMigrateAccount(pihm.cdm.PartitionMap[lastPartitionMapId], pihm.cdm.NewBrokerMap[lastBrokerMapId], pihm.cdm.DeleteBrokerQue[lastBrokerMapId])
	pihm.MixUpdateMigrateTx(pihm.cdm.PartitionMap[lastPartitionMapId], NewActiveBroker, NewIdleBroker)
	pihm.pbftNode.CurChain.Txpool.GetUnlocked()

	for shardIdx := uint64(0); shardIdx < pihm.pbftNode.pbftChainConfig.ShardNums; shardIdx++ {
		if shardIdx == pihm.pbftNode.ShardID { //skip current shard
			continue
		}
		ast := message.AccountStateAndTx{
			Addrs:        pihm.migSup.AddrSend[shardIdx],
			AccountState: pihm.migSup.StateSend[shardIdx],
			Txs:          pihm.migSup.TxsSend[shardIdx],
			FromShard:    pihm.pbftNode.ShardID,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.C2C_MixUpdateTxMsg, aByte)
		networks.TcpDial(send_msg, pihm.pbftNode.ip_nodeTable[shardIdx][0]) //intra-shard TX  send to shard i
		pihm.pbftNode.pl.Plog.Printf("The message to shard %d is sent，account num：%d，state num：%d，TX num：%d\n",
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
		networks.TcpDial(send_msg, pihm.pbftNode.ip_nodeTable[params.DeciderShard][0]) // cross-shard TX。send to supervisor
		pihm.pbftNode.pl.Plog.Printf("The message to DeciderShard is sent，交易数：%d\n", len(i2ctx.Txs))
	}

	pihm.migSup.ResetMigrateSupport() // Reset
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
	for s, at := range pihm.cdm.AccountStateTx {
		for i, state := range at.AccountState { // Iterate over the status(and split-state) sent by a shard
			addr := state.Address
			if _, ok := pihm.cdm.DeleteBrokerQue[len(pihm.cdm.DeleteBrokerQue)-1].AllocateMap[addr]; ok {
				pihm.pbftNode.pl.Plog.Printf("The expired splt-state sent from shard %d was received", s)
			}
			if _, ok := pihm.cdm.ReceivedNewAccountState[addr]; !ok { // new
				pihm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
			} else {
				//Add split-split-state to the existing split-state
				pihm.cdm.ReceivedNewAccountState[addr].AggregateAccount(at.AccountState[i])
			}
		}
		pihm.cdm.ReceivedNewTx = append(pihm.cdm.ReceivedNewTx, at.Txs...)
	}
	pihm.pbftNode.pl.Plog.Printf("S%dN%d : Received %d accounts and %d transactions\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, len(pihm.cdm.ReceivedNewAccountState), len(pihm.cdm.ReceivedNewTx))
	// propose, send all txs to other nodes in shard
	pihm.pbftNode.CurChain.Txpool.AddTxs2Pool(pihm.cdm.ReceivedNewTx) // Txpool添加交易

	pihm.pbftNode.pl.Plog.Println("The size of the transaction pool after receiving transactions from other shards;", len(pihm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range pihm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
		pihm.pbftNode.CurChain.Aslock.Lock()
		pihm.pbftNode.CurChain.AccountState[key] = val //Collect all received status statistics
		pihm.pbftNode.CurChain.Aslock.Unlock()
	}

	atm := message.AccountMixUpdateMsg{
		SingleBroker: pihm.cdm.NewBrokerMap[pihm.cdm.AccountTransferRound],
		PartitionMap: pihm.cdm.PartitionMap[pihm.cdm.AccountTransferRound],
		Addrs:        atmaddr,
		AccountState: atmAs,
		ATid:         uint64(len(pihm.cdm.NewBrokerMap)),
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
	pihm.pbftNode.pl.Plog.Println("更新Broker和AllocationMap")
	if len(asm.PartitionMap) != 0 {
		pihm.pbftNode.CurChain.Update_AllocationMap(asm.PartitionMap)
		pihm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", len(asm.PartitionMap))
	}

	if len(asm.SingleBroker.AddressQue) != 0 {
		delBroker := pihm.pbftNode.CurChain.Update_BrokerMap(asm.SingleBroker)
		pihm.pbftNode.pl.Plog.Printf("The current fragment S%dN%d needs to remove %d active accounts", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, len(delBroker.AddressQue))
	}

	pihm.pbftNode.CurChain.AddAccounts(asm.AccountState) // add split-state

	if uint64(len(pihm.cdm.NewBrokerMap)) != asm.ATid {
		pihm.cdm.NewBrokerMap = append(pihm.cdm.NewBrokerMap, asm.SingleBroker)
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

	if pihm.pbftNode.NodeID == pihm.pbftNode.view { //only main node
		// Send all permission brokers to A-Shard
		PermitInfoSet := make([]message.MixUpdateInfoStruct, 0)
		for idx, addr := range asm.SingleBroker.AddressQue {
			//pihm.pbftNode.pl.Plog.Println(addr)
			shard := asm.SingleBroker.AllocateMap[addr]
			if utils.InMapKeys(pihm.pbftNode.ShardID, shard) {
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
			pihm.pbftNode.pl.Plog.Printf("S%dN%d : account num=%d send MixUpdate permission to A-Shard\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, len(PermitInfoSet))
		}
	}
}
