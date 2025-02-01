package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

type PBFTInsideHandleModule struct {
	cdm      *dataSupport.DataSupport
	pbftNode *PbftConsensusNode
	migSup   *dataSupport.MigrateSupport
}

// propose request with different types
func (pihm *PBFTInsideHandleModule) HandleinPropose() (bool, *message.Request) {
	if pihm.cdm.MixUpdateCode != 0 {
		pihm.pbftNode.pl.Plog.Printf("-- MixUpdate %d 提议...", pihm.cdm.MixUpdateCode)
		pihm.sendMixUpdateReady()
		for !pihm.getMixUpdateReady() { // 得所有的分片都同意patition
			//pihm.pbftNode.pl.Plog.Println("Partition Not Ready, Please Wait...")
			time.Sleep(time.Second)
		}
		// send accounts and txs
		pihm.MixUpdateMigrateMain() // 完成账户以及交易的迁移
		// propose a partition
		for !pihm.getMixUpdateCollectOver() {
			//pihm.pbftNode.pl.Plog.Println("Collect Not Over...")
			time.Sleep(time.Second)
		}
		pihm.pbftNode.pl.Plog.Println("MixUpdate Ready & Collect Over")
		return pihm.proposeMixUpdate()
	}

	// ELSE: propose a block
	pihm.pbftNode.pl.Plog.Println("打包前剩余交易数：", len(pihm.pbftNode.CurChain.Txpool.TxQueue))
	block := pihm.pbftNode.CurChain.GenerateBlock() // 从txpool里面打包tx
	pihm.pbftNode.pl.Plog.Println("打包后--剩余交易数：", len(pihm.pbftNode.CurChain.Txpool.TxQueue))
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r
}

// the diy operation in preprepare
func (pihm *PBFTInsideHandleModule) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isMixUpdateReq := ppmsg.RequestMsg.RequestType == message.MixUpdateReq

	if isMixUpdateReq {
		// after some checking
		pihm.pbftNode.pl.Plog.Printf("S%dN%d :PrePrepare阶段： a MixUpdate block\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
	} else {
		// the request is a block
		if pihm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			pihm.pbftNode.pl.Plog.Printf("S%dN%d : NOT A VALID BLOCK!!!\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
			return false
		}
	}
	pihm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
	//pihm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	pihm.pbftNode.requestPool.Store(string(ppmsg.Digest), ppmsg.RequestMsg)
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (pihm *PBFTInsideHandleModule) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (pihm *PBFTInsideHandleModule) HandleinCommit(cmsg *message.Commit) bool {
	rs, _ := pihm.pbftNode.requestPool.Load(string(cmsg.Digest))
	r, _ := rs.(*message.Request)
	//fmt.Println("请求的类型：", r.RequestType, r.RequestType == message.MixUpdateReq)
	// requestType ...
	if r.RequestType == message.MixUpdateReq {
		// if a partition Requst ...
		atm := message.DecodeAccountMixUpdateMsg(r.Msg.Content)
		pihm.accountMixUpdate_do(atm) // 重置状态
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	pihm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, block.Header.Number, pihm.pbftNode.CurChain.CurrentBlock.Header.Number)
	pihm.pbftNode.CurChain.AddBlock(block)
	pihm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, block.Header.Number)
	pihm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if pihm.pbftNode.NodeID == pihm.pbftNode.view {
		//for ActiveBridgeQue, _ := pihm.pbftNode.CurChain.Get_BridgeMap(); len(ActiveBridgeQue) == 0; {
		//	time.Sleep(time.Second)
		//}

		pihm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send broker confirm txs at height = %d \n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, block.Header.Number)
		// generate brokertxs and collect txs excuted
		txExcuted := make([]*core.Transaction, 0)
		broker1Txs := make([]*core.Transaction, 0)
		broker2Txs := make([]*core.Transaction, 0)

		// generate block infos
		for _, tx := range block.Body {
			isBroker1Tx := tx.Sender == tx.OriginalSender    //发送方=初始发送方 -> 是tx1，（从初始发送方发给broker）
			isBroker2Tx := tx.Recipient == tx.FinalRecipient //接收方=最终接收方 -> 是tx2，（从broker发给最终接收方）

			//senderIsInshard := senderMainShard == pihm.pbftNode.ShardID       // 发送方分片=当前分片
			//recipientIsInshard := recipientMainShard == pihm.pbftNode.ShardID // 接收方分片=当前分片

			senderIsExist, senderMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(tx.Sender)
			recipientIsExist, recipientMainShard := pihm.pbftNode.CurChain.Get_AllocationMap(tx.Recipient)
			NowShard := pihm.pbftNode.ShardID

			NowABridge := pihm.pbftNode.CurChain.Get_BridgeMap()
			if len(NowABridge.ActiveBridge.AddressQue) == 0 {
				pihm.pbftNode.pl.Plog.Println("ActiveBridgeQue为空")
				log.Panic("ActiveBridgeQue为空")
			}
			_, senderAssoShards := NowABridge.GetBridgeShardList(tx.Sender)
			if len(senderAssoShards) == 0 {
				senderAssoShards = []uint64{senderMainShard}
			}

			_, recipientAssoShards := NowABridge.GetBridgeShardList(tx.Recipient)
			if len(recipientAssoShards) == 0 {
				recipientAssoShards = []uint64{recipientMainShard}
			}
			senderIsBridge := NowABridge.IsBridge(tx.Sender) != -1
			recipientIsBridge := NowABridge.IsBridge(tx.Recipient) != -1
			if senderIsBridge && len(senderAssoShards) <= 1 {
				pihm.pbftNode.pl.Plog.Println("Bridge分布错误：", senderIsBridge, " - ", senderIsExist, senderMainShard, senderAssoShards)
				log.Panic()
			}
			if recipientIsBridge && len(recipientAssoShards) <= 1 {
				pihm.pbftNode.pl.Plog.Println("Bridge分布错误：", recipientIsBridge, " - ", recipientIsExist, recipientMainShard, recipientAssoShards)
				log.Panic()
			}

			if tx.RawTxHash == nil { // -------------  tx.RawTxHash == nil 说明没有经过拆分
				if senderIsBridge || recipientIsBridge { // 有broker
					if senderIsBridge && !recipientIsBridge && recipientMainShard != NowShard { // 发送方是broker，但是接收方不是broker
						pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
						pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
						log.Panic("OriTx：recipient not in NowShard")
					}
					if !senderIsBridge && recipientIsBridge && senderMainShard != NowShard { // 接收方是broker，但发送方不是
						pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
						pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
						log.Panic("OriTx：sender not in NowShard")
					}
					if senderIsBridge && recipientIsBridge { // 2个都是broker
						if !(utils.InIntList(NowShard, senderAssoShards) && utils.InIntList(NowShard, recipientAssoShards)) { //当前分片必须在2个bridge的assoshard中
							pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
							pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
							log.Panic("OriTx：NowShard is not intersection shard")
						}
					}
				} else { // 没有broker，则说明应该是片内交易
					if senderMainShard != NowShard || recipientMainShard != NowShard { //但是其中 发送方/接收方 有一个不在本分片
						pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
						pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
						log.Panic("OriTx： - without broker") //说明出现错误
					}
				}
			} else { //  -------------  经过拆分的交易
				if isBroker1Tx && senderMainShard != NowShard { //Sender → Broker
					if senderIsBridge && !utils.InIntList(NowShard, senderAssoShards) {
						pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
						pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
						log.Panic("Err tx1 Sender → Broker") // 从初始发送方发给broker，但是发送方不在当前分片 -> 说明初始发送方和broker跨了分片，因此报错
					}
					if !senderIsBridge {
						pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
						pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
						log.Panic("Err tx1 Sender → Broker")
					}
				}
				if isBroker2Tx && recipientMainShard != NowShard { // 说明是broker发给了本分片的接收者 （但是接收方不在broker的分片）
					if recipientIsBridge && !utils.InIntList(NowShard, recipientAssoShards) {
						pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
						pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
						log.Panic("Err tx2 Broker → Recipient")
					}
					if !recipientIsBridge {
						pihm.pbftNode.pl.Plog.Println("Sender - IsBridge:", senderIsBridge, "Exist:", senderIsExist, "MainShard:", senderMainShard, "AssoShard:", senderAssoShards)
						pihm.pbftNode.pl.Plog.Println("Recipient - IsBridge:", recipientIsBridge, "Exist:", recipientIsExist, "MainShard:", recipientMainShard, "AssoShard:", recipientAssoShards)
						log.Panic("Err tx2 Broker → Recipient")
					}
				}
			}

			if isBroker2Tx {
				broker2Txs = append(broker2Txs, tx)
			} else if isBroker1Tx {
				broker1Txs = append(broker1Txs, tx)
			} else {
				if tx.RawTxHash != nil {
					log.Panic("片内交易，却有RawTxHash")
				}
				if tx.RootTxHash != nil {
					log.Panic("片内交易，却有RootTxHash")
				}
				txExcuted = append(txExcuted, tx)
			}
		}
		// send seqID
		for sid := uint64(0); sid < pihm.pbftNode.pbftChainConfig.ShardNums; sid++ {
			if sid == pihm.pbftNode.ShardID { // 跳过当前
				continue
			}
			sii := message.SeqIDinfo{
				SenderShardID: pihm.pbftNode.ShardID,
				SenderSeq:     pihm.pbftNode.sequenceID,
			}
			sByte, err := json.Marshal(sii)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CSeqIDinfo, sByte)
			networks.TcpDial(msg_send, pihm.pbftNode.ip_nodeTable[sid][0])
			pihm.pbftNode.pl.Plog.Printf("S%dN%d : send sequence ids to S%dN0\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, sid)
		}
		// send txs executed in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   txExcuted,
			Broker1Txs:      broker1Txs,
			Broker2Txs:      broker2Txs,
			Epoch:           int(pihm.cdm.AccountTransferRound),
			SenderShardID:   pihm.pbftNode.ShardID,
			ProposeTime:     r.ReqTime,
			CommitTime:      time.Now(),
			//PendingTXs:      pihm.pbftNode.CurChain.Txpool.TxQueue, // 发送挂起交易
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		pihm.pbftNode.pl.Plog.Printf("S%dN%d : sended txs to DeciderShard\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
		networks.TcpDial(msg_send, pihm.pbftNode.ip_nodeTable[params.DeciderShard][0])
		pihm.pbftNode.pl.Plog.Printf("S%dN%d : sended executed txs\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)

		pihm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Broker1 Txs in this block",
			"# of Broker2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Broker1 Txs) (Duration: Broker1 proposed -> Broker1 Commit)",
			"SUM of confirm latency (ms, Broker2 Txs) (Duration: Broker2 proposed -> Broker2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(pihm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(broker1Txs)),
			strconv.Itoa(len(broker2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker2Txs, bim.CommitTime), 10),
		}
		pihm.pbftNode.writeCSVline(metricName, metricVal)
		pihm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	return true
}

func (pihm *PBFTInsideHandleModule) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (pihm *PBFTInsideHandleModule) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		pihm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		pihm.pbftNode.pl.Plog.Printf("S%dN%d : ----需要块高度%d~%d的区块\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID, som.SeqStartHeight, som.SeqEndHeight)
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest { // 请求补上block
				pihm.pbftNode.pl.Plog.Printf("S%dN%d : 请求补上block\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
				b := core.DecodeB(r.Msg.Content)
				pihm.pbftNode.CurChain.AddBlock(b)
			} else {
				pihm.pbftNode.pl.Plog.Printf("S%dN%d : 请求补上partition\n", pihm.pbftNode.ShardID, pihm.pbftNode.NodeID)
				atm := message.DecodeAccountMixUpdateMsg(r.Msg.Content) // 请求补上partition
				pihm.accountMixUpdate_do(atm)
			}
		}
		pihm.pbftNode.sequenceID = som.SeqEndHeight + 1
		pihm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
