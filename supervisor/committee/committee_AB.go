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
	abLock            sync.Mutex
	abGraph           *partition_split.ABState
	abLastRunningTime time.Time
	abFreq            int

	ABrokers      *broker.ABrokers
	abridgeLock   sync.RWMutex
	AllocationMap map[string]uint64
	alloLock      sync.RWMutex

	bridgeConfirm1Pool map[string]*message.ABrokersMag1Confirm
	bridgeConfirm2Pool map[string]*message.ABrokersMag2Confirm
	bridgeTxPool       []*core.Transaction
	bridgeModuleLock   sync.Mutex

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss              *signal.StopSignal // to control the stop message sending
	IpNodeTable     map[uint64]map[uint64]string
	PermitBrokerMap map[string]map[uint64]bool
	PermitBrokerQue map[string]int
	PermitBrokerNum int
	//UpdateTime       time.Time
	IsUpdateFinished bool

	corssShardNum int
	innerShardNum int

	shard_txNum []int

	//PartitionMap         map[string]uint64
	NewActiveBroker, NewIdleBroker *core.SingleBroker
	reSplitTx                      []*core.Transaction

	TxFromCShard      int
	reSplitTxCount    int
	Re_reSplitTxCount int
}

func NewABCommitteeMod(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog,
	csvFilePath []string, indexBegin, indexEnd, dataNum, batchNum, abFrequency int) *ABCommitteeMod {
	mg := new(partition_split.ABState)
	mg.Init_ABState(0.5, params.MaxIterations, params.ShardNum) // 新加参数
	ABrokers := new(broker.ABrokers)
	ABrokers.NewABrokers(nil)

	NewActiveBroker := core.InitBrokerAddr(params.ABrokersInitNum, params.InitBrokerMod)
	ABrokers.AdaptiveBroker.UpdateBrokerMap(NewActiveBroker)

	return &ABCommitteeMod{
		csvPath:            csvFilePath,
		indexBegin:         indexBegin,
		indexEnd:           indexEnd,
		dataTotalNum:       dataNum,
		batchDataNum:       batchNum,
		nowDataNum:         0,
		corssShardNum:      0,
		innerShardNum:      0,
		abGraph:            mg,
		AllocationMap:      make(map[string]uint64),
		abFreq:             abFrequency,
		abLastRunningTime:  time.Time{},
		bridgeConfirm1Pool: make(map[string]*message.ABrokersMag1Confirm),
		bridgeConfirm2Pool: make(map[string]*message.ABrokersMag2Confirm),
		bridgeTxPool:       make([]*core.Transaction, 0),
		ABrokers:           ABrokers,

		IpNodeTable:      Ip_nodeTable,
		Ss:               Ss,
		sl:               sl,
		PermitBrokerQue:  make(map[string]int),
		PermitBrokerMap:  make(map[string]map[uint64]bool),
		PermitBrokerNum:  0,
		IsUpdateFinished: true,

		shard_txNum: make([]int, params.ShardNum),

		NewActiveBroker:   core.CreateSingleBroker(),
		NewIdleBroker:     core.CreateSingleBroker(),
		reSplitTx:         make([]*core.Transaction, 0),
		TxFromCShard:      0,
		reSplitTxCount:    0,
		Re_reSplitTxCount: 0,

		curEpoch: 0,
		abCnt:    0,
	}
}

func (ccm *ABCommitteeMod) MsgSendingControl() {
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	for idx := ccm.indexBegin; idx <= ccm.indexEnd; idx++ {
		ccm.sl.Slog.Println("Read the dataset: ", ccm.csvPath[idx])
		txfile, err := os.Open(ccm.csvPath[idx])
		if err != nil {
			log.Panic(err)
		}
		defer txfile.Close()
		reader := csv.NewReader(txfile)

		for {
			data, err1 := reader.Read() // Read Tx in csvPath (read one by one!)
			if err1 == io.EOF {
				break // 进入下一层，idx++
			}
			if err1 != nil {
				log.Panic(err1)
			}
			if tx, ok := data2tx(data, uint64(ccm.nowDataNum)); ok { // Convert to Tx structure
				txlist = append(txlist, tx) //Continuously adding data to txlist
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
			if len(txlist) == ccm.batchDataNum || ccm.nowDataNum == ccm.dataTotalNum { // Achieve a batch
				// set the algorithm timer begins
				if ccm.abLastRunningTime.IsZero() {
					ccm.abLastRunningTime = time.Now()
				}
				itx := ccm.dealTxByABrokers(txlist)
				ccm.txSending(itx)
				// reset the variants about tx sending
				txlist = make([]*core.Transaction, 0) // 清空了txlist
				ccm.Ss.StopGap_Reset()
			}

			if !ccm.abLastRunningTime.IsZero() && time.Since(ccm.abLastRunningTime) >= time.Duration(ccm.abFreq)*time.Second {
				ccm.abLock.Lock()
				ccm.abCnt++
				ccm.AddPendingTXs() // Add suspended transactions to the graph (note: not use!!!)
				mmap := ccm.abGraph.ABPartition_flpa()
				//mmap, _ := ccm.abGraph.CLPA_Partition()
				ccm.sl.Slog.Println("====== UpdateAllocationMap")
				ccm.UpdateAllocationMap(mmap)

				SelBroker := core.CreateSingleBroker()
				NewActiveBroker := core.CreateSingleBroker()
				NewIdleBroker := core.CreateSingleBroker()
				if ccm.IsUpdateFinished && params.ABrokersAddNum != 0 {
					SelBroker = ccm.abGraph.ABSplit(params.ABrokersAddNum, true)
					NewActiveBroker, NewIdleBroker, _ = ccm.ABrokers.AdaptiveBroker.PreUpdateBrokerMap(SelBroker)
				}
				if len(SelBroker.AddressQue) == 0 {
					ccm.NewActiveBroker = ccm.ABrokers.AdaptiveBroker.ActiveBroker
					ccm.NewIdleBroker = ccm.ABrokers.AdaptiveBroker.IdleBroker
				} else {
					ccm.NewActiveBroker = NewActiveBroker
					ccm.NewIdleBroker = NewIdleBroker
				}

				ccm.sl.Slog.Println("send MixUpdate(1), ", len(mmap))
				ccm.abMixUpdateMsgSend(mmap, SelBroker, false)
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
			ccm.AddPendingTXs()
			mmap := ccm.abGraph.ABPartition_flpa()
			//mmap, _ := ccm.abGraph.CLPA_Partition()
			//mmap := make(map[string]uint64)
			ccm.UpdateAllocationMap(mmap)

			SelBroker := core.CreateSingleBroker()
			NewActiveBroker := core.CreateSingleBroker()
			NewIdleBroker := core.CreateSingleBroker()
			if ccm.IsUpdateFinished && params.ABrokersAddNum != 0 {
				SelBroker = ccm.abGraph.ABSplit(params.ABrokersAddNum, true)
				NewActiveBroker, NewIdleBroker, _ = ccm.ABrokers.AdaptiveBroker.PreUpdateBrokerMap(SelBroker)
			}
			if len(SelBroker.AddressQue) == 0 {
				ccm.NewActiveBroker = ccm.ABrokers.AdaptiveBroker.ActiveBroker
				ccm.NewIdleBroker = ccm.ABrokers.AdaptiveBroker.IdleBroker
			} else {
				ccm.NewActiveBroker = NewActiveBroker
				ccm.NewIdleBroker = NewIdleBroker
			}

			ccm.sl.Slog.Println("send MixUpdate(2), ", len(mmap))
			ccm.abMixUpdateMsgSend(mmap, SelBroker, false)
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
	ccm.sl.Slog.Printf("print - TxFromCShard:%d,reSplitTxCount:%d, Re_reSplitTxCount:%d", ccm.TxFromCShard, ccm.reSplitTxCount, ccm.Re_reSplitTxCount)
}

func (ccm *ABCommitteeMod) dealTxByABrokers(txs []*core.Transaction) (itxs []*core.Transaction) {
	itxs = make([]*core.Transaction, 0)
	bridgeRawMegs := make([]*message.ABrokersRawMeg, 0)
	for _, tx := range txs {
		senderMainShard, senderShardList := ccm.getActiveAllo(tx.Sender)
		recipientMainShard, recipientShardList := ccm.getActiveAllo(tx.Recipient)
		if tx.RawTxHash != nil {
			if !utils.EqualByteSlices(tx.RawTxHash, tx.RootTxHash) {
				ccm.Re_reSplitTxCount++
			} else {
				ccm.reSplitTxCount++
			}
		}

		if s := utils.GetIntersection(senderShardList, recipientShardList); len(s) == 0 {
			targetBrokerAddr := ccm.ABrokers.AdaptiveBroker.FindIntersectionBroker(senderMainShard, recipientMainShard, ccm.NewActiveBroker)
			rootTxHash := tx.TxHash
			if tx.RootTxHash != nil {
				rootTxHash = tx.RootTxHash
			}
			brokerRawMeg := &message.ABrokersRawMeg{
				Tx:         tx,
				Broker:     targetBrokerAddr, // 选中一个broker
				RootTxHash: rootTxHash,
			}
			bridgeRawMegs = append(bridgeRawMegs, brokerRawMeg)
			ccm.corssShardNum++
		} else {
			itxs = append(itxs, tx)
			ccm.innerShardNum++
		}
	}
	if len(bridgeRawMegs) != 0 {
		ccm.handleABrokersRawMag(bridgeRawMegs)
	}
	return itxs
}

func (ccm *ABCommitteeMod) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ { // 0 ~ ShardNum
				it := message.InjectTxs{
					Epoch:     ccm.abCnt,
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, ccm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		ccm.abLock.Lock()

		senderMainShard, senderShardList := ccm.getNewAllo(tx.Sender)
		recipientMainShard, recipientShardList := ccm.getNewAllo(tx.Recipient)
		IntersecList := utils.GetIntersection(senderShardList, recipientShardList)
		if len(IntersecList) == 0 {
			ccm.reSplitTx = append(ccm.reSplitTx, tx) // Cross shard Tx, needs to be re split
		} else {
			rand.Seed(time.Now().UnixNano())
			r := rand.Intn(len(IntersecList))
			toShard := IntersecList[r]

			NewActiveBroker, NewIdleBroker := ccm.NewActiveBroker, ccm.NewIdleBroker
			if len(NewActiveBroker.AddressQue) == 0 {
				NewActiveBroker, NewIdleBroker = ccm.ABrokers.AdaptiveBroker.ActiveBroker, ccm.ABrokers.AdaptiveBroker.IdleBroker
			}
			if core.NewestThan(tx.Sender, tx.Recipient, NewActiveBroker, NewIdleBroker) == 1 {
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
}

func (ccm *ABCommitteeMod) abMixUpdateMsgSend(mmap map[string]uint64, nab *core.SingleBroker, isInit bool) {
	nbq := message.A2C_MixUpdateMsgStruct{
		PartitionModified: mmap,
		NewSingleBroker:   nab,
		IsInit:            isInit,
	}
	nbqByte, err := json.Marshal(nbq)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.A2C_MixUpdateMsg, nbqByte)
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		// send to worker shards
		ccm.sl.Slog.Printf("%d Partition item and %d New Broker, send to shard%d.\n", len(nbq.PartitionModified), len(nbq.NewSingleBroker.AddressQue), shard)
		networks.TcpDial(send_msg, ccm.IpNodeTable[shard][0])
	}
	ccm.sl.Slog.Println("Supervisor: all MixUpdate message has been sent. ")
}

func (ccm *ABCommitteeMod) abReset() {
	ccm.alloLock.Lock()
	defer ccm.alloLock.Unlock()
	ccm.abGraph = new(partition_split.ABState)
	ccm.abGraph.Init_ABState(0.5, params.MaxIterations, params.ShardNum)
	for key, val := range ccm.AllocationMap {
		ccm.abGraph.VertexAlloMap[partition_split.Vertex{Addr: key}] = int(val)
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
	itxs := ccm.dealTxByABrokers(itct.Txs)
	ccm.txSending(itxs)
}

func (ccm *ABCommitteeMod) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("从C-Shard %d Received transaction epoch %d.\n", b.SenderShardID, b.Epoch)
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
		ccm.abGraph.AddEdge(partition_split.Vertex{Addr: tx.Sender}, partition_split.Vertex{Addr: tx.Recipient})
	}
	for _, b1tx := range b.Broker1Txs {
		ccm.abGraph.AddEdge(partition_split.Vertex{Addr: b1tx.OriginalSender}, partition_split.Vertex{Addr: b1tx.FinalRecipient})
	}
	ccm.abLock.Unlock()
}

func (ccm *ABCommitteeMod) AddPendingTXs() {
	for i := 0; i < params.PTxWeight; i++ {
		for _, ptx := range ccm.bridgeTxPool {
			isBroker1Tx := ptx.Sender == ptx.OriginalSender
			isBroker2Tx := ptx.Recipient == ptx.FinalRecipient

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
	confirm1s := make([]*message.ABrokersMag1Confirm, 0)
	confirm2s := make([]*message.ABrokersMag2Confirm, 0)
	ccm.bridgeModuleLock.Lock()
	for _, tx := range txs {
		if confirm1, ok := ccm.bridgeConfirm1Pool[string(tx.TxHash)]; ok { // If this and the transaction are in the pool, add
			confirm1s = append(confirm1s, confirm1)
		}
		if confirm2, ok := ccm.bridgeConfirm2Pool[string(tx.TxHash)]; ok {
			confirm2s = append(confirm2s, confirm2)
		}
	}
	ccm.bridgeModuleLock.Unlock()

	if len(confirm1s) != 0 {
		ccm.handleTx1ConfirmMag(confirm1s)
	}

	if len(confirm2s) != 0 {
		ccm.handleTx2ConfirmMag(confirm2s)
	}
}

// 根据从各个分片收到的许可Broker 更新A-Shard的bridge
func (ccm *ABCommitteeMod) HandleMixUpdateInfo(b *message.MixUpdateInfoMsg) {
	for _, permit := range b.MixUpdateInfo {
		ccm.PermitBrokerQue[permit.SenderAddress] = permit.Index
		ccm.PermitBrokerMap[permit.SenderAddress] = permit.AssoShard // Receive all Broker accounts for a shard and add up this shard
	}
	ccm.sl.Slog.Printf("收到来自分片%d的bridge permission\n", b.FromShard)
	ccm.PermitBrokerNum += 1
	if ccm.PermitBrokerNum == params.ShardNum { //Received all shards
		ccm.sl.Slog.Printf("收集完毕，更新bridge len=%d \n", len(ccm.PermitBrokerMap))

		var pairs []Pair
		AddrQue := make([]string, 0)
		for k, v := range ccm.PermitBrokerQue {
			pairs = append(pairs, Pair{k, v})
		}
		// Sort the slices
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].Value < pairs[j].Value })
		for idx, pa := range pairs {
			if idx != pa.Value {
				for i, p := range pairs {
					fmt.Println("idx:", i, p.Key, p.Value)
					log.Panic()
				}
			}
			AddrQue = append(AddrQue, pa.Key)
		}

		SelBroker := &core.SingleBroker{
			AddressQue:  AddrQue,
			AllocateMap: ccm.PermitBrokerMap,
		}
		SelBroker.CheckBroker()

		deleteBroker := ccm.ABrokers.AdaptiveBroker.UpdateBrokerMap(SelBroker)
		ccm.sl.Slog.Printf("删除 %d 个IdleABroker", len(deleteBroker.AllocateMap))

		ccm.PermitBrokerQue = make(map[string]int)
		ccm.PermitBrokerMap = make(map[string]map[uint64]bool)
		ccm.PermitBrokerNum = 0
		ccm.NewActiveBroker = core.CreateSingleBroker()
		ccm.NewIdleBroker = core.CreateSingleBroker()
		ccm.IsUpdateFinished = true
	}
}

func (ccm *ABCommitteeMod) handleABrokersType1Mes(bridgeType1Megs []*message.ABrokersType1Meg) {
	tx1s := make([]*core.Transaction, 0)
	for _, brokerType1Meg := range bridgeType1Megs {
		ctx := brokerType1Meg.RawMeg.Tx                                                     // cross-chain Tx
		tx1 := core.NewTransaction(ctx.Sender, brokerType1Meg.Broker, ctx.Value, ctx.Nonce) //
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
		confirm1 := &message.ABrokersMag1Confirm{
			RawMeg:  brokerType1Meg.RawMeg,
			Tx1Hash: tx1.TxHash, // 新建tx的hash
		}
		ccm.bridgeModuleLock.Lock()
		ccm.bridgeConfirm1Pool[string(tx1.TxHash)] = confirm1
		ccm.bridgeModuleLock.Unlock()
	}
	ccm.txSending(tx1s)
	ccm.sl.Slog.Printf("create TX Sender->ABrokers, send brokerTx1 * %d...", len(tx1s))
}

func (ccm *ABCommitteeMod) handleABrokersType2Mes(brokerType2Megs []*message.ABrokersType2Meg) {
	tx2s := make([]*core.Transaction, 0)
	for _, mes := range brokerType2Megs {
		ctx := mes.RawMeg.Tx
		tx2 := core.NewTransaction(mes.Broker, ctx.Recipient, ctx.Value, ctx.Nonce)
		tx2.OriginalSender = ctx.Sender
		tx2.FinalRecipient = ctx.Recipient

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
		confirm2 := &message.ABrokersMag2Confirm{
			RawMeg:  mes.RawMeg,
			Tx2Hash: tx2.TxHash,
		}
		ccm.bridgeModuleLock.Lock()
		ccm.bridgeConfirm2Pool[string(tx2.TxHash)] = confirm2
		ccm.bridgeModuleLock.Unlock()
	}
	//ccm.sl.Slog.Println("TxSending 5")
	ccm.txSending(tx2s)
	ccm.sl.Slog.Printf("create TX ABrokers->Recipient, send brokerTx2 * %d...", len(tx2s))
}

// get the digest of rawMeg
func (ccm *ABCommitteeMod) getABrokersRawMagDigest(r *message.ABrokersRawMeg) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

func (ccm *ABCommitteeMod) handleABrokersRawMag(bridgeRawMags []*message.ABrokersRawMeg) {
	bridgeSet := ccm.ABrokers
	brokerType1Mags := make([]*message.ABrokersType1Meg, 0)
	ccm.sl.Slog.Println("broker receive ctx ", len(bridgeRawMags))
	ccm.bridgeModuleLock.Lock()
	for _, meg := range bridgeRawMags {
		bridgeSet.ABrokersRawMegs[string(ccm.getABrokersRawMagDigest(meg))] = meg // 加入map

		brokerType1Mag := &message.ABrokersType1Meg{
			RawMeg:   meg,
			Hcurrent: 0,
			Broker:   meg.Broker,
		}
		brokerType1Mags = append(brokerType1Mags, brokerType1Mag)
	}
	ccm.bridgeModuleLock.Unlock()
	ccm.handleABrokersType1Mes(brokerType1Mags)
}

func (ccm *ABCommitteeMod) handleTx1ConfirmMag(mag1confirms []*message.ABrokersMag1Confirm) {
	brokerType2Mags := make([]*message.ABrokersType2Meg, 0)
	b := ccm.ABrokers

	ccm.sl.Slog.Printf("收到brokerTx1 * %d 的confirm...", len(mag1confirms))
	ccm.bridgeModuleLock.Lock()
	for _, mag1confirm := range mag1confirms {
		RawMeg := mag1confirm.RawMeg
		_, ok := b.ABrokersRawMegs[string(ccm.getABrokersRawMagDigest(RawMeg))]
		if !ok {
			ccm.sl.Slog.Println("raw message is not exited,tx1 confirms failure !")
			continue
		}

		rootTxHash := RawMeg.RootTxHash
		b.RawTx2ABrokersTx[string(rootTxHash)] = append(b.RawTx2ABrokersTx[string(rootTxHash)], string(mag1confirm.Tx1Hash))

		brokerType2Mag := &message.ABrokersType2Meg{
			Broker: RawMeg.Broker,
			RawMeg: RawMeg,
		}
		brokerType2Mags = append(brokerType2Mags, brokerType2Mag)
	}
	ccm.bridgeModuleLock.Unlock()
	ccm.handleABrokersType2Mes(brokerType2Mags)
}

func (ccm *ABCommitteeMod) handleTx2ConfirmMag(mag2confirms []*message.ABrokersMag2Confirm) {
	b := ccm.ABrokers

	ccm.sl.Slog.Printf("receive brokerTx2 * %d confirm...", len(mag2confirms))
	num := 0
	splitNum := 0
	ccm.bridgeModuleLock.Lock()
	for _, mag2confirm := range mag2confirms {
		RawMeg := mag2confirm.RawMeg
		rootTxHash := RawMeg.RootTxHash
		b.RawTx2ABrokersTx[string(rootTxHash)] = append(b.RawTx2ABrokersTx[string(rootTxHash)], string(mag2confirm.Tx2Hash))

		if len(b.RawTx2ABrokersTx[string(rootTxHash)]) == 2 {
			num++
		} else {
			splitNum++
		}
	}
	ccm.bridgeModuleLock.Unlock()
	ccm.sl.Slog.Printf("Complete the addition of cross shard Tx * %d / Resplited-TX * %d", num, splitNum)
}
