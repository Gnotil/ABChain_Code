package dataSupport

import (
	"blockEmulator/core"
)

type MigrateSupport struct {
	TxsSend   map[uint64][]*core.Transaction // 每个分片 txs
	TxsCross  []*core.Transaction            // 跨分片Tx，发给A-Shard
	AddrSend  map[uint64][]string
	StateSend map[uint64][]*core.AccountState
}

func NewMigrateSupport() *MigrateSupport {
	return &MigrateSupport{
		TxsSend:   make(map[uint64][]*core.Transaction, 0), // 每个分片 txs
		TxsCross:  make([]*core.Transaction, 0),            // 跨分片Tx，发给A-Shard
		AddrSend:  make(map[uint64][]string, 0),
		StateSend: make(map[uint64][]*core.AccountState, 0),
	}
}

func (ms *MigrateSupport) ResetMigrateSupport() {
	ms.TxsSend = make(map[uint64][]*core.Transaction, 0) // 每个分片 txs
	ms.TxsCross = make([]*core.Transaction, 0)           // 跨分片Tx，发给A-Shard
	ms.AddrSend = make(map[uint64][]string, 0)
	ms.StateSend = make(map[uint64][]*core.AccountState, 0)
}
