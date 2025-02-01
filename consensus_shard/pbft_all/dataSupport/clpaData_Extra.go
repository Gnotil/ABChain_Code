package dataSupport

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"sync"
)

type DataSupport struct {
	PartitionMap            []map[string]uint64                   // record the modified map from the decider(s)
	NewBridgeMap            []*core.SingleBridge                  // 记录bridge和高关联分片
	DeleteBridgeQue         []*core.SingleBridge                  // 记录bridge和高关联分片
	AccountTransferRound    uint64                                // denote how many times accountTransfer do
	ReceivedNewAccountState map[string]*core.AccountState         // the new accountState From other Shards
	ReceivedNewTx           []*core.Transaction                   // new transactions from other shards' pool
	AccountStateTx          map[uint64]*message.AccountStateAndTx // the map of accountState and transactions, pool
	MixUpdateCode           int                                   // judge nextEpoch is partition or not

	MixUpdateReadyMap map[uint64]bool // judge whether all shards has done all txs
	ReadyLock         sync.Mutex      // lock for ready

	ReadySeqMap  map[uint64]uint64 // record the seqid when the shard is ready
	ReadySeqLock sync.Mutex        // lock for seqMap

	CollectOver bool       // judge whether all txs is collected or not
	CollectLock sync.Mutex // lock for collect
}

func NewCLPADataSupport() *DataSupport {
	ABridges := new(core.Bridge)
	ABridges.NewBridge()
	return &DataSupport{
		PartitionMap:            make([]map[string]uint64, 0),
		NewBridgeMap:            make([]*core.SingleBridge, 0),
		DeleteBridgeQue:         make([]*core.SingleBridge, 0),
		AccountTransferRound:    0,
		ReceivedNewAccountState: make(map[string]*core.AccountState),
		ReceivedNewTx:           make([]*core.Transaction, 0),
		AccountStateTx:          make(map[uint64]*message.AccountStateAndTx),
		MixUpdateCode:           0,
		MixUpdateReadyMap:       make(map[uint64]bool),
		CollectOver:             false,
		ReadySeqMap:             make(map[uint64]uint64),
	}
}
