package committee

import (
	"blockEmulator/broker"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition_split"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	_ "math/big"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ab committee operations
type ABCommitteeMod struct {
	csvPath      []string
	indexBegin   int
	indexEnd     int
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	curEpoch int32
	abCnt    int32

	// additional variants
	abLock  sync.Mutex
	abGraph *partition_split.ABState // 新加参数
	//modifiedMap       map[string]uint64
	abLastRunningTime time.Time
	abFreq            int

	//broker related  attributes avatar
	ABridges      *broker.ABridges
	abridgeLock   sync.RWMutex
	AllocationMap map[string]uint64
	alloLock      sync.RWMutex

	bridgeConfirm1Pool map[string]*message.ABridgesMag1Confirm
	bridgeConfirm2Pool map[string]*message.ABridgesMag2Confirm
	bridgeTxPool       []*core.Transaction
	bridgeModuleLock   sync.Mutex
	//targetBridge       broker.SelBridge

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss              *signal.StopSignal // to control the stop message sending
	IpNodeTable     map[uint64]map[uint64]string
	PermitBridgeMap map[string]map[uint64]bool
	PermitBridgeQue map[string]int
	PermitBridgeNum int
	//UpdateTime       time.Time
	IsUpdateFinished bool

	corssShardNum int
	innerShardNum int

	shard_txNum []int

	//PartitionMap         map[string]uint64
	NewActiveBridge, NewIdleBridge *core.SingleBridge
	reSplitTx                      []*core.Transaction

	TxFromCShard      int
	reSplitTxCount    int
	Re_reSplitTxCount int
}

func NewABCommitteeMod(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog,
	csvFilePath []string, indexBegin, indexEnd, dataNum, batchNum, abFrequency int) *ABCommitteeMod {
	mg := new(partition_split.ABState)
	mg.Init_ABState(0.5, params.MaxIterations, params.ShardNum) // 新加参数
	ABridges := new(broker.ABridges)
	ABridges.NewABridges(nil)

	NewActiveBridge := core.InitBrokerAddr(params.ABridgesInitNum, params.InitBridgeMod)
	ABridges.AdaptiveBridge.UpdateBridgeMap(NewActiveBridge)

	return &ABCommitteeMod{
		csvPath:            csvFilePath,
		indexBegin:         indexBegin,
		indexEnd:           indexEnd,
		dataTotalNum:       dataNum,
		batchDataNum:       batchNum,
		nowDataNum:         0,
		corssShardNum:      0,
		innerShardNum:      0,
		abGraph:            mg, // 新加参数
		AllocationMap:      make(map[string]uint64),
		abFreq:             abFrequency,
		abLastRunningTime:  time.Time{},
		bridgeConfirm1Pool: make(map[string]*message.ABridgesMag1Confirm),
		bridgeConfirm2Pool: make(map[string]*message.ABridgesMag2Confirm),
		bridgeTxPool:       make([]*core.Transaction, 0),
		ABridges:           ABridges,

		IpNodeTable:     Ip_nodeTable,
		Ss:              Ss,
		sl:              sl,
		PermitBridgeQue: make(map[string]int),
		PermitBridgeMap: make(map[string]map[uint64]bool),
		PermitBridgeNum: 0,
		//UpdateTime:       time.Time{},
		IsUpdateFinished: true,

		shard_txNum: make([]int, params.ShardNum),

		//PartitionMap: make(map[string]uint64),
		NewActiveBridge:   core.CreateSingleBridge(),
		NewIdleBridge:     core.CreateSingleBridge(),
		reSplitTx:         make([]*core.Transaction, 0),
		TxFromCShard:      0,
		reSplitTxCount:    0,
		Re_reSplitTxCount: 0,

		curEpoch: 0,
		abCnt:    0,
	}
}

func (ccm *ABCommitteeMod) MsgSendingControl() {
	//abCnt := 0
	//addReSplitTxCount := 0
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	for idx := ccm.indexBegin; idx <= ccm.indexEnd; idx++ {
		ccm.sl.Slog.Println("读取数据集: ", ccm.csvPath[idx])
		txfile, err := os.Open(ccm.csvPath[idx]) // 打开csvPath
		if err != nil {
			log.Panic(err)
		}
		defer txfile.Close()
		reader := csv.NewReader(txfile) // 构建reader

		for {
			data, err1 := reader.Read() // 读 csvPath 里的 Tx (一条一条地读！)
			if err1 == io.EOF {
				break // 进入下一层，idx++
			}
			if err1 != nil {
				log.Panic(err1)
			}
			if tx, ok := data2tx(data, uint64(ccm.nowDataNum)); ok { // 转化为Tx结构
				txlist = append(txlist, tx) //不停地往txlist添加数据
				ccm.nowDataNum++
			} else {
				continue
			}

			if len(ccm.reSplitTx) != 0 {
				txlist = append(txlist, ccm.reSplitTx...) // 添加
				//addReSplitTxCount += len(ccm.reSplitTx)
				ccm.reSplitTx = make([]*core.Transaction, 0) //清空
			}

			// batch sending condition
			if len(txlist) == ccm.batchDataNum || ccm.nowDataNum == ccm.dataTotalNum { // 达到一个batch
				// set the algorithm timer begins
				if ccm.abLastRunningTime.IsZero() {
					ccm.abLastRunningTime = time.Now()
				}
				itx := ccm.dealTxByABridges(txlist)
				ccm.txSending(itx)
				// reset the variants about tx sending
				txlist = make([]*core.Transaction, 0) // 清空了txlist
				ccm.Ss.StopGap_Reset()
			}

			if !ccm.abLastRunningTime.IsZero() && time.Since(ccm.abLastRunningTime) >= time.Duration(ccm.abFreq)*time.Second {
				// 时间到该重新ab_Partition了
				ccm.abLock.Lock()
				ccm.abCnt++
				ccm.AddPendingTXs() // 把挂起交易添加到图
				mmap := ccm.abGraph.ABPartition_flpa()
				//mmap, _ := ccm.abGraph.CLPA_Partition()
				//mmap, _ := ccm.abGraph.CLPA_Partition()
				ccm.sl.Slog.Println("====== UpdateAllocationMap")
				ccm.UpdateAllocationMap(mmap)

				SelBridge := core.CreateSingleBridge()
				NewActiveBridge := core.CreateSingleBridge()
				NewIdleBridge := core.CreateSingleBridge()
				if ccm.IsUpdateFinished && params.ABridgesAddNum != 0 {
					SelBridge = ccm.abGraph.ABSplit(params.ABridgesAddNum, true)
					NewActiveBridge, NewIdleBridge, _ = ccm.ABridges.AdaptiveBridge.PreUpdateBridgeMap(SelBridge)
				}
				//ccm.PartitionMap = mmap
				if len(SelBridge.AddressQue) == 0 {
					ccm.NewActiveBridge = ccm.ABridges.AdaptiveBridge.ActiveBridge
					ccm.NewIdleBridge = ccm.ABridges.AdaptiveBridge.IdleBridge
				} else {
					ccm.NewActiveBridge = NewActiveBridge
					ccm.NewIdleBridge = NewIdleBridge
				}

				ccm.sl.Slog.Println("send MixUpdate(1), ", len(mmap))
				ccm.abMixUpdateMsgSend(mmap, SelBridge, false)
				ccm.IsUpdateFinished = false
				ccm.sl.Slog.Println("send MixUpdate success(1)")

				ccm.abReset()
				ccm.abLock.Unlock()

				for atomic.LoadInt32(&ccm.curEpoch) != int32(ccm.abCnt) {
					time.Sleep(time.Second)
				}
				ccm.abLastRunningTime = time.Now()
				ccm.sl.Slog.Println("Next CLPA epoch begins. ")
			}

			if ccm.nowDataNum == ccm.dataTotalNum { // 跑完
				idx = ccm.indexEnd + 1
				break
			}
		}
	}
	// all transactions are sent. keep sending partition message...
	for !ccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(ccm.abLastRunningTime) >= time.Duration(ccm.abFreq)*time.Second {
			ccm.abLock.Lock()
			ccm.abCnt++
			ccm.AddPendingTXs() // 把挂起交易添加到图
			mmap := ccm.abGraph.ABPartition_flpa()
			//mmap, _ := ccm.abGraph.CLPA_Partition()
			//mmap := make(map[string]uint64)
			ccm.UpdateAllocationMap(mmap)

			SelBridge := core.CreateSingleBridge()
			NewActiveBridge := core.CreateSingleBridge()
			NewIdleBridge := core.CreateSingleBridge()
			if ccm.IsUpdateFinished && params.ABridgesAddNum != 0 {
				SelBridge = ccm.abGraph.ABSplit(params.ABridgesAddNum, true)
				NewActiveBridge, NewIdleBridge, _ = ccm.ABridges.AdaptiveBridge.PreUpdateBridgeMap(SelBridge)
			}
			if len(SelBridge.AddressQue) == 0 {
				ccm.NewActiveBridge = ccm.ABridges.AdaptiveBridge.ActiveBridge
				ccm.NewIdleBridge = ccm.ABridges.AdaptiveBridge.IdleBridge
			} else {
				ccm.NewActiveBridge = NewActiveBridge
				ccm.NewIdleBridge = NewIdleBridge
			}

			ccm.sl.Slog.Println("send MixUpdate(2), ", len(mmap))
			ccm.abMixUpdateMsgSend(mmap, SelBridge, false)
			ccm.IsUpdateFinished = false
			ccm.sl.Slog.Println("send MixUpdate success(2)")

			ccm.abReset()
			ccm.abLock.Unlock()

			ccm.abLastRunningTime = time.Now()
			for atomic.LoadInt32(&ccm.curEpoch) != int32(ccm.abCnt) {
				time.Sleep(time.Second)
			}
			ccm.abLastRunningTime = time.Now()
			ccm.sl.Slog.Println("Next CLPA epoch begins. ")
		}
	}
	ccm.sl.Slog.Printf("最后打印 - TxFromCShard:%d,reSplitTxCount:%d, Re_reSplitTxCount:%d", ccm.TxFromCShard, ccm.reSplitTxCount, ccm.Re_reSplitTxCount)
}

func (ccm *ABCommitteeMod) dealTxByABridges(txs []*core.Transaction) (itxs []*core.Transaction) {
	itxs = make([]*core.Transaction, 0)
	bridgeRawMegs := make([]*message.ABridgesRawMeg, 0)
	for _, tx := range txs {
		senderMainShard, senderShardList := ccm.getActiveAllo(tx.Sender)
		recipientMainShard, recipientShardList := ccm.getActiveAllo(tx.Recipient)
		if tx.RawTxHash != nil {
			if !utils.EqualByteSlices(tx.RawTxHash, tx.RootTxHash) {
				ccm.Re_reSplitTxCount++
			} else {
				ccm.reSplitTxCount++
			}
			//ccm.sl.Slog.Println("拆分过的交易，需要再次拆分")
			//senderMainShard, senderShardList = ccm.getNewAllo(tx.Sender)
			//recipientMainShard, recipientShardList = ccm.getNewAllo(tx.Recipient)
		}

		if s := utils.GetIntersection(senderShardList, recipientShardList); len(s) == 0 {
			// 收发双方不能通过 ActiveBridge 沟通 (此时，如果是ExpiredBridge发起的跨分片交易，也需要通过ActiveBridge拆分)
			targetBridgeAddr := ccm.ABridges.AdaptiveBridge.FindIntersectionBridge(senderMainShard, recipientMainShard, ccm.NewActiveBridge)
			rootTxHash := tx.TxHash
			if tx.RootTxHash != nil {
				rootTxHash = tx.RootTxHash
			}
			brokerRawMeg := &message.ABridgesRawMeg{
				Tx:         tx,
				Bridge:     targetBridgeAddr, // 选中一个broker
				RootTxHash: rootTxHash,
			}
			bridgeRawMegs = append(bridgeRawMegs, brokerRawMeg)
			ccm.corssShardNum++
		} else { // 能conn
			itxs = append(itxs, tx) // 普通片内交易
			ccm.innerShardNum++
		}
	}
	if len(bridgeRawMegs) != 0 { // 需要broker处理
		ccm.handleABridgesRawMag(bridgeRawMegs) // 转交broker处理
	}
	return itxs
}

func (ccm *ABCommitteeMod) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) { // 每隔params.InjectSpeed执行一次，最后也要执行一次
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ { // 0 ~ ShardNum
				it := message.InjectTxs{
					Epoch:     ccm.abCnt,
					Txs:       sendToShard[sid], // 这里应该是空？？ => idx > 0才执行，所以先会执行下面的语句
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, ccm.IpNodeTable[sid][0]) // 发给其他sid 的node0？？
				//ccm.sl.Slog.Printf("txSend 分片 %d，交易数 %d", sid, len(sendToShard[sid]))
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx] // 获取txlist的内容
		ccm.abLock.Lock()

		//senderMainShard, senderShardList := ccm.getActiveAllo(tx.Sender)
		//recipientMainShard, recipientShardList := ccm.getActiveAllo(tx.Recipient)
		//if tx.RawTxHash != nil {
		//	senderMainShard, senderShardList = ccm.getNewAllo(tx.Sender)
		//	recipientMainShard, recipientShardList = ccm.getNewAllo(tx.Recipient)
		//}
		senderMainShard, senderShardList := ccm.getNewAllo(tx.Sender)
		recipientMainShard, recipientShardList := ccm.getNewAllo(tx.Recipient)
		IntersecList := utils.GetIntersection(senderShardList, recipientShardList)
		if len(IntersecList) == 0 {
			ccm.reSplitTx = append(ccm.reSplitTx, tx)
			//ccm.reSplitTxCount++
			//ccm.sl.Slog.Println("tx.Sender==tx.OriginalSender: ", tx.Sender == tx.OriginalSender)
			//ccm.sl.Slog.Println("tx.Recipient==tx.FinalRecipient: ", tx.Recipient == tx.FinalRecipient)
			//ccm.sl.Slog.Println("senderShardList: ", senderShardList)
			//ccm.sl.Slog.Println("recipientShardList: ", recipientShardList)
			//ccm.sl.Slog.Panic("跨分片Tx，需要重新拆分")
		} else {
			rand.Seed(time.Now().UnixNano())
			r := rand.Intn(len(IntersecList))
			toShard := IntersecList[r]

			NewActiveBridge, NewIdleBridge := ccm.NewActiveBridge, ccm.NewIdleBridge
			if len(NewActiveBridge.AddressQue) == 0 {
				NewActiveBridge, NewIdleBridge = ccm.ABridges.AdaptiveBridge.ActiveBridge, ccm.ABridges.AdaptiveBridge.IdleBridge
			}
			if core.NewestThan(tx.Sender, tx.Recipient, NewActiveBridge, NewIdleBridge) == 1 { //Sender更加新，应该发给Recipient
				if utils.InIntList(recipientMainShard, IntersecList) {
					toShard = recipientMainShard
				}
			} else {
				if utils.InIntList(senderMainShard, IntersecList) {
					toShard = senderMainShard
				}
			}
			//Tx是需要被发送给 非broker？
			sendToShard[toShard] = append(sendToShard[toShard], tx)
			//目标分片交易数+1
			ccm.shard_txNum[toShard] += 1
		}
		ccm.abLock.Unlock()
	}
	//if len(reSplitTx) > 0 {
	//	ccm.sl.Slog.Println("跨分片Tx，需要重新拆分，长度为", len(reSplitTx))
	//	reTx := ccm.dealTxByABridges(reSplitTx)
	//	ccm.sl.Slog.Println("TxSending 2")
	//	ccm.txSending(reTx)
	//}
}

func (ccm *ABCommitteeMod) abMixUpdateMsgSend(mmap map[string]uint64, nab *core.SingleBridge, isInit bool) {
	nbq := message.A2C_MixUpdateMsgStruct{
		PartitionModified: mmap,
		NewSingleBridge:   nab,
		IsInit:            isInit,
	}
	nbqByte, err := json.Marshal(nbq)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.A2C_MixUpdateMsg, nbqByte)
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		// send to worker shards
		ccm.sl.Slog.Printf("%d个Partition信息与%d个New Bridge 发送给分片%d.\n", len(nbq.PartitionModified), len(nbq.NewSingleBridge.AddressQue), shard)
		networks.TcpDial(send_msg, ccm.IpNodeTable[shard][0])
	}
	ccm.sl.Slog.Println("Supervisor: all MixUpdate message has been sent. ")
}

func (ccm *ABCommitteeMod) abReset() {
	ccm.alloLock.Lock()
	defer ccm.alloLock.Unlock()
	//ccm.shard_txNum = make([]int, params.ShardNum)
	ccm.abGraph = new(partition_split.ABState)
	ccm.abGraph.Init_ABState(0.5, params.MaxIterations, params.ShardNum) // 新加参数
	for key, val := range ccm.AllocationMap {
		//ccm.abGraph.VertexShardMap[partition_split.Vertex{Addr: key}] = int(val) // partitionMap从AllocationMap取值
		//ccm.abGraph.VertexShardMap.Store(partition_split.Vertex{Addr: key}, int(val))
		ccm.abGraph.VertexAlloMap[partition_split.Vertex{Addr: key}] = int(val)
		//ccm.abGraph.PartitionMap[partition_split.Vertex{Addr: key}] = int(val)
	}
}

// for ab_Broker committee, it only handle the extra CInner2CrossTx message.
func (ccm *ABCommitteeMod) HandleOtherMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	if msgType != message.CrInner2CrossTx {
		return
	}
	itct := new(message.InnerTx2CrossTx)
	err := json.Unmarshal(content, itct)
	if err != nil {
		log.Panic()
	}
	ccm.sl.Slog.Printf("处理 Other Message from shard %d, len %d", itct.FromShard, len(itct.Txs))
	ccm.TxFromCShard += len(itct.Txs)
	itxs := ccm.dealTxByABridges(itct.Txs)
	ccm.txSending(itxs)
}

func (ccm *ABCommitteeMod) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("从C-Shard %d 收到交易 epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}

	// add createConfirm
	txs := make([]*core.Transaction, 0)
	txs = append(txs, b.Broker1Txs...)
	txs = append(txs, b.Broker2Txs...)
	ccm.createConfirm(txs)

	ccm.abLock.Lock()
	for _, tx := range b.InnerShardTxs {
		//SenderIsBridge := ccm.IsBridge(tx.Sender) == 1
		//RecipientIsBridge := ccm.IsBridge(tx.Recipient) == 1 // active
		//if SenderIsBridge || RecipientIsBridge {             // 跳过有broker的
		//	continue
		//}
		ccm.abGraph.AddEdge(partition_split.Vertex{Addr: tx.Sender}, partition_split.Vertex{Addr: tx.Recipient})
	}
	for _, b1tx := range b.Broker1Txs {
		ccm.abGraph.AddEdge(partition_split.Vertex{Addr: b1tx.OriginalSender}, partition_split.Vertex{Addr: b1tx.FinalRecipient})
	}
	//for _, ptx := range b.PendingTXs {
	//	ccm.bridgeTxPool = append(ccm.bridgeTxPool, ptx)
	//}
	//ccm.bridgeTxPool = b.PendingTXs
	ccm.abLock.Unlock()
}

func (ccm *ABCommitteeMod) AddPendingTXs() {
	for i := 0; i < params.PTxWeight; i++ {
		for _, ptx := range ccm.bridgeTxPool {
			isBroker1Tx := ptx.Sender == ptx.OriginalSender    //发送方=初始发送方 -> 是tx1，（从初始发送方发给broker）
			isBroker2Tx := ptx.Recipient == ptx.FinalRecipient //接收方=最终接收方 -> 是tx2，（从broker发给最终接收方）

			if isBroker1Tx {
				ccm.abGraph.AddEdge(partition_split.Vertex{Addr: ptx.OriginalSender}, partition_split.Vertex{Addr: ptx.FinalRecipient})
			} else if isBroker2Tx {
				continue
			} else {
				ccm.abGraph.AddEdge(partition_split.Vertex{Addr: ptx.Sender}, partition_split.Vertex{Addr: ptx.Recipient})
			}
		}
	}
}

func (ccm *ABCommitteeMod) createConfirm(txs []*core.Transaction) {
	confirm1s := make([]*message.ABridgesMag1Confirm, 0)
	confirm2s := make([]*message.ABridgesMag2Confirm, 0)
	ccm.bridgeModuleLock.Lock()
	for _, tx := range txs {
		if confirm1, ok := ccm.bridgeConfirm1Pool[string(tx.TxHash)]; ok { // 如果这个及交易在pool里，添加
			confirm1s = append(confirm1s, confirm1)
		}
		if confirm2, ok := ccm.bridgeConfirm2Pool[string(tx.TxHash)]; ok {
			confirm2s = append(confirm2s, confirm2)
		}
	}
	ccm.bridgeModuleLock.Unlock()

	if len(confirm1s) != 0 {
		ccm.handleTx1ConfirmMag(confirm1s) // 处理 confirm
	}

	if len(confirm2s) != 0 {
		ccm.handleTx2ConfirmMag(confirm2s)
	}
}

// 根据从各个分片收到的许可Bridge 更新A-Shard的bridge
func (ccm *ABCommitteeMod) HandleMixUpdateInfo(b *message.MixUpdateInfoMsg) {
	for _, permit := range b.MixUpdateInfo {
		ccm.PermitBridgeQue[permit.SenderAddress] = permit.Index
		ccm.PermitBridgeMap[permit.SenderAddress] = permit.AssoShard // 收到一个分片的所有Bridge账户，累加这个分片
	}
	ccm.sl.Slog.Printf("收到来自分片%d的bridge permission\n", b.FromShard)
	ccm.PermitBridgeNum += 1
	if ccm.PermitBridgeNum == params.ShardNum { //收到所有分片
		ccm.sl.Slog.Printf("收集完毕，更新bridge len=%d \n", len(ccm.PermitBridgeMap))

		// 将map的键值对复制到slice中
		var pairs []Pair
		AddrQue := make([]string, 0)
		for k, v := range ccm.PermitBridgeQue {
			pairs = append(pairs, Pair{k, v})
		}
		// 对slice进行排序
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].Value < pairs[j].Value }) // 升序
		for idx, pa := range pairs {
			if idx != pa.Value {
				for i, p := range pairs {
					fmt.Println("idx:", i, p.Key, p.Value)
					log.Panic()
				}
			}
			AddrQue = append(AddrQue, pa.Key)
		}

		SelBridge := &core.SingleBridge{
			AddressQue:  AddrQue,
			AllocateMap: ccm.PermitBridgeMap,
		}
		SelBridge.CheckBridge()

		deleteBridge := ccm.ABridges.AdaptiveBridge.UpdateBridgeMap(SelBridge)
		ccm.sl.Slog.Printf("删除 %d 个IdleABridge", len(deleteBridge.AllocateMap))

		ccm.PermitBridgeQue = make(map[string]int)
		ccm.PermitBridgeMap = make(map[string]map[uint64]bool) // 清空
		ccm.PermitBridgeNum = 0
		//ccm.PartitionMap = make(map[string]uint64)
		ccm.NewActiveBridge = core.CreateSingleBridge()
		ccm.NewIdleBridge = core.CreateSingleBridge()
		ccm.IsUpdateFinished = true
		//ccm.sl.Slog.Println("++++++++++++++++++++++++++++++++++++++ActiveBridge")
		//for _, i := range ccm.ABridges.AdaptiveBridge.ActiveBridge.AddressQue {
		//	ccm.sl.Slog.Println(i)
		//}
		//ccm.sl.Slog.Println("++++++++++++++++++++++++++++++++++++++IdleBridge")
		//for _, i := range ccm.ABridges.AdaptiveBridge.IdleBridge.AddressQue {
		//	ccm.sl.Slog.Println(i)
		//}
	}
}

func (ccm *ABCommitteeMod) handleABridgesType1Mes(bridgeType1Megs []*message.ABridgesType1Meg) {
	tx1s := make([]*core.Transaction, 0)
	for _, brokerType1Meg := range bridgeType1Megs {
		ctx := brokerType1Meg.RawMeg.Tx                                                     // cross-chain Tx
		tx1 := core.NewTransaction(ctx.Sender, brokerType1Meg.Bridge, ctx.Value, ctx.Nonce) // 新建一个sender到broker的Tx
		tx1.OriginalSender = ctx.Sender
		tx1.FinalRecipient = ctx.Recipient

		tx1.RawTxHash = make([]byte, len(ctx.TxHash)) // Raw
		copy(tx1.RawTxHash, ctx.TxHash)

		if ctx.RootTxHash == nil { // root
			tx1.RootTxHash = make([]byte, len(ctx.TxHash))
			copy(tx1.RootTxHash, ctx.TxHash)
		} else {
			tx1.RootTxHash = make([]byte, len(ctx.RootTxHash))
			copy(tx1.RootTxHash, ctx.RootTxHash)
		}

		tx1s = append(tx1s, tx1)
		confirm1 := &message.ABridgesMag1Confirm{
			RawMeg:  brokerType1Meg.RawMeg,
			Tx1Hash: tx1.TxHash, // 新建tx的hash
		}
		ccm.bridgeModuleLock.Lock()
		ccm.bridgeConfirm1Pool[string(tx1.TxHash)] = confirm1 // 加入 待确认的TX1 pool
		ccm.bridgeModuleLock.Unlock()
	}
	//ccm.sl.Slog.Println("TxSending 4")
	ccm.txSending(tx1s)
	ccm.sl.Slog.Printf("构建Sender->ABridges, 发送brokerTx1 * %d...", len(tx1s))
	//ccm.sl.Slog.Println("ABridgesType1Mes received by shard,  add brokerTx1 len ", len(tx1s))
}

func (ccm *ABCommitteeMod) handleABridgesType2Mes(brokerType2Megs []*message.ABridgesType2Meg) {
	tx2s := make([]*core.Transaction, 0)
	for _, mes := range brokerType2Megs {
		ctx := mes.RawMeg.Tx
		tx2 := core.NewTransaction(mes.Bridge, ctx.Recipient, ctx.Value, ctx.Nonce) // 新建从broker到接收方的Tx
		tx2.OriginalSender = ctx.Sender
		tx2.FinalRecipient = ctx.Recipient
		//SenderIsBridge = true

		tx2.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx2.RawTxHash, ctx.TxHash)

		if ctx.RootTxHash == nil { // root
			tx2.RootTxHash = make([]byte, len(ctx.TxHash))
			copy(tx2.RootTxHash, ctx.TxHash)
		} else {
			tx2.RootTxHash = make([]byte, len(ctx.RootTxHash))
			copy(tx2.RootTxHash, ctx.RootTxHash)
		}

		tx2s = append(tx2s, tx2)
		confirm2 := &message.ABridgesMag2Confirm{
			RawMeg:  mes.RawMeg,
			Tx2Hash: tx2.TxHash,
		}
		ccm.bridgeModuleLock.Lock()
		ccm.bridgeConfirm2Pool[string(tx2.TxHash)] = confirm2
		ccm.bridgeModuleLock.Unlock()
	}
	//ccm.sl.Slog.Println("TxSending 5")
	ccm.txSending(tx2s)
	ccm.sl.Slog.Printf("构建ABridges->Recipient, 发送brokerTx2 * %d...", len(tx2s))
	//ccm.sl.Slog.Println("brokerTx2 add to pool len ", len(tx2s))
}

// get the digest of rawMeg
func (ccm *ABCommitteeMod) getABridgesRawMagDigest(r *message.ABridgesRawMeg) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

func (ccm *ABCommitteeMod) handleABridgesRawMag(bridgeRawMags []*message.ABridgesRawMeg) {
	bridgeSet := ccm.ABridges
	brokerType1Mags := make([]*message.ABridgesType1Meg, 0)
	ccm.sl.Slog.Println("broker receive ctx ", len(bridgeRawMags))
	ccm.bridgeModuleLock.Lock()
	for _, meg := range bridgeRawMags {
		bridgeSet.ABridgesRawMegs[string(ccm.getABridgesRawMagDigest(meg))] = meg // 加入map

		brokerType1Mag := &message.ABridgesType1Meg{ // 将bridgeRawMags打包成brokerType1Mag
			RawMeg:   meg,
			Hcurrent: 0, // 当前高度？
			Bridge:   meg.Bridge,
		}
		brokerType1Mags = append(brokerType1Mags, brokerType1Mag)
	}
	ccm.bridgeModuleLock.Unlock()
	ccm.handleABridgesType1Mes(brokerType1Mags)
}

func (ccm *ABCommitteeMod) handleTx1ConfirmMag(mag1confirms []*message.ABridgesMag1Confirm) {
	brokerType2Mags := make([]*message.ABridgesType2Meg, 0)
	b := ccm.ABridges

	//ccm.sl.Slog.Println("receive confirm  brokerTx1 len ", len(mag1confirms))
	ccm.sl.Slog.Printf("收到brokerTx1 * %d 的confirm...", len(mag1confirms))
	ccm.bridgeModuleLock.Lock()
	for _, mag1confirm := range mag1confirms {
		RawMeg := mag1confirm.RawMeg
		_, ok := b.ABridgesRawMegs[string(ccm.getABridgesRawMagDigest(RawMeg))]
		if !ok {
			ccm.sl.Slog.Println("raw message is not exited,tx1 confirms failure !")
			continue
		}
		//b.RawTx2ABridgesTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2ABridgesTx[string(RawMeg.Tx.TxHash)], string(mag1confirm.Tx1Hash))

		rootTxHash := RawMeg.RootTxHash
		b.RawTx2ABridgesTx[string(rootTxHash)] = append(b.RawTx2ABridgesTx[string(rootTxHash)], string(mag1confirm.Tx1Hash))

		brokerType2Mag := &message.ABridgesType2Meg{
			//ABridges: ccm.targetBridge.Address, // 这里的bridge的选择问题
			Bridge: RawMeg.Bridge, // 这里的bridge的选择问题
			RawMeg: RawMeg,
		}
		brokerType2Mags = append(brokerType2Mags, brokerType2Mag)
	}
	ccm.bridgeModuleLock.Unlock()
	ccm.handleABridgesType2Mes(brokerType2Mags)
}

func (ccm *ABCommitteeMod) handleTx2ConfirmMag(mag2confirms []*message.ABridgesMag2Confirm) {
	b := ccm.ABridges
	//ccm.sl.Slog.Println("receive confirm  brokerTx2 len ", len(mag2confirms))
	ccm.sl.Slog.Printf("收到brokerTx2 * %d 的confirm...", len(mag2confirms))
	num := 0
	splitNum := 0
	ccm.bridgeModuleLock.Lock()
	for _, mag2confirm := range mag2confirms {
		RawMeg := mag2confirm.RawMeg
		rootTxHash := RawMeg.RootTxHash
		b.RawTx2ABridgesTx[string(rootTxHash)] = append(b.RawTx2ABridgesTx[string(rootTxHash)], string(mag2confirm.Tx2Hash))

		if len(b.RawTx2ABridgesTx[string(rootTxHash)]) == 2 {
			num++
		} else {
			//ccm.sl.Slog.Println("存在splited-TXs, len =", len(b.RawTx2ABridgesTx[string(rootTxHash)]))
			splitNum++
		}
	}
	ccm.bridgeModuleLock.Unlock()
	//ccm.sl.Slog.Println("finish ctx with adding tx1 and tx2 to txpool,len", num)
	ccm.sl.Slog.Printf("完成跨分片Tx * %d / Resplited-TX * %d的添加", num, splitNum)
}
