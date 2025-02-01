package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/utils"
	"encoding/json"
	"log"
)

type PBFTOutsideHandleModule struct {
	cdm      *dataSupport.DataSupport
	pbftNode *PbftConsensusNode
}

func (pohm *PBFTOutsideHandleModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CSeqIDinfo:
		pohm.handleSeqIDinfos(content)
	case message.CInject:
		pohm.handleInjectTx(content)

	case message.A2C_MixUpdateMsg:
		pohm.handleMixUpdateMsg(content)
	case message.C2C_MixUpdateTxMsg:
		pohm.handleMixUpdateTxsFromOtherShard(content)
	case message.C2C_MixUpdateReady:
		pohm.handleMixUpdateReady(content)

	default:
	}
	return true
}

// receive SeqIDinfo
func (pohm *PBFTOutsideHandleModule) handleSeqIDinfos(content []byte) {
	sii := new(message.SeqIDinfo)
	err := json.Unmarshal(content, sii)
	if err != nil {
		log.Panic(err)
	}
	pohm.pbftNode.pl.Plog.Printf("S%dN%d : has received SeqIDinfo from shard %d, the senderSeq is %d\n", pohm.pbftNode.ShardID, pohm.pbftNode.NodeID, sii.SenderShardID, sii.SenderSeq)
	pohm.pbftNode.seqMapLock.Lock()
	pohm.pbftNode.seqIDMap[sii.SenderShardID] = sii.SenderSeq // message from other shard
	pohm.pbftNode.seqMapLock.Unlock()
	pohm.pbftNode.pl.Plog.Printf("S%dN%d : has handled SeqIDinfo msg\n", pohm.pbftNode.ShardID, pohm.pbftNode.NodeID)
}

func (pohm *PBFTOutsideHandleModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	pohm.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	pohm.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, epoch: %d txs: %d \n", pohm.pbftNode.ShardID, pohm.pbftNode.NodeID, it.Epoch, len(it.Txs))

	// Save the accounts of all incoming transactions (because they go through the broker, all incoming transactions are on-chip)
	pohm.pbftNode.CurChain.Aslock.Lock()
	for _, tx := range it.Txs {
		senderIsExist, senderMainShard := pohm.pbftNode.CurChain.Get_AllocationMap(tx.Sender)          //senderIsExist
		recipientIsExist, recipientMainShard := pohm.pbftNode.CurChain.Get_AllocationMap(tx.Recipient) //recipientIsExist
		if _, ok := pohm.pbftNode.CurChain.AccountState[tx.Sender]; !ok && !senderIsExist && senderMainShard == pohm.pbftNode.ShardID {
			state := core.ConstructAccount(tx.Sender, false)
			pohm.pbftNode.CurChain.AccountState[tx.Sender] = state
		}
		if _, ok := pohm.pbftNode.CurChain.AccountState[tx.Recipient]; !ok && !recipientIsExist && recipientMainShard == pohm.pbftNode.ShardID {
			state := core.ConstructAccount(tx.Recipient, false)
			pohm.pbftNode.CurChain.AccountState[tx.Recipient] = state
		}
	}
	pohm.pbftNode.CurChain.Aslock.Unlock()
}

/*
============================================== split
*/
func (pohm *PBFTOutsideHandleModule) handleMixUpdateMsg(content []byte) {
	pm := new(message.A2C_MixUpdateMsgStruct)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	pohm.cdm.PartitionMap = append(pohm.cdm.PartitionMap, pm.PartitionModified)
	pohm.cdm.NewBrokerMap = append(pohm.cdm.NewBrokerMap, pm.NewSingleBroker)
	// 设置MixUpdateCode
	if len(pm.PartitionModified) != 0 && len(pm.NewSingleBroker.AddressQue) == 0 {
		pohm.cdm.MixUpdateCode = 1
	} else if len(pm.PartitionModified) == 0 && len(pm.NewSingleBroker.AddressQue) != 0 {
		pohm.cdm.MixUpdateCode = 2
	} else if len(pm.PartitionModified) != 0 && len(pm.NewSingleBroker.AddressQue) != 0 {
		pohm.cdm.MixUpdateCode = 3
	}
	pohm.pbftNode.pl.Plog.Printf("S%dN%d : Received MixUpdate message, update code %d\n", pohm.pbftNode.ShardID, pohm.pbftNode.NodeID, pohm.cdm.MixUpdateCode)
	pohm.pbftNode.CurChain.Aslock.Lock()
	if pm.IsInit { // When initializing, you need to create a state
		for addr, shard := range pm.NewSingleBroker.AllocateMap {
			if !utils.InMapKeys(pohm.pbftNode.ShardID, shard) {
				_, ShardList := utils.GetMapShard(shard)
				pohm.pbftNode.pl.Plog.Printf("The current shard %d is not the associated shard of the account", pohm.pbftNode.ShardID, ShardList)
				log.Panic("")
			}
			mainShard, _ := utils.GetMapShard(shard)
			if _, ok := pohm.pbftNode.CurChain.AccountState[addr]; !ok && mainShard == pohm.pbftNode.ShardID {
				state := core.ConstructAccount(addr, false)
				pohm.pbftNode.CurChain.AccountState[addr] = state
			}
		}
	}
	pohm.pbftNode.CurChain.Aslock.Unlock()
}

// wait for other shards' last rounds are over
func (pohm *PBFTOutsideHandleModule) handleMixUpdateReady(content []byte) {
	pr := new(message.MixUpdateReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	pohm.cdm.ReadyLock.Lock()
	pohm.cdm.MixUpdateReadyMap[pr.FromShard] = true
	pohm.cdm.ReadyLock.Unlock()

	pohm.pbftNode.seqMapLock.Lock()
	pohm.cdm.ReadySeqMap[pr.FromShard] = pr.NowSeqID
	pohm.pbftNode.seqMapLock.Unlock()

	pohm.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

// when the message from other shard arriving, it should be added into the message pool
func (pohm *PBFTOutsideHandleModule) handleMixUpdateTxsFromOtherShard(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	pohm.cdm.AccountStateTx[at.FromShard] = at
	pohm.pbftNode.pl.Plog.Printf("S%dN%d: Number of states received by shard S%d : %d", pohm.pbftNode.ShardID, pohm.pbftNode.NodeID, at.FromShard, len(at.AccountState))

	if len(pohm.cdm.AccountStateTx) == int(pohm.pbftNode.pbftChainConfig.ShardNums)-1 {
		pohm.cdm.CollectLock.Lock()
		pohm.cdm.CollectOver = true
		pohm.cdm.CollectLock.Unlock()
		pohm.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", pohm.pbftNode.ShardID, pohm.pbftNode.NodeID)
	}
}
