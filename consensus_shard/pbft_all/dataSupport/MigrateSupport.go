package dataSupport

import (
	"blockEmulator/core"
)

type MigrateSupport struct {
	TxsSend   map[uint64][]*core.Transaction
	TxsCross  []*core.Transaction
	AddrSend  map[uint64][]string
	StateSend map[uint64][]*core.AccountState
}

func NewMigrateSupport() *MigrateSupport {
	return &MigrateSupport{
		TxsSend:   make(map[uint64][]*core.Transaction, 0),
		TxsCross:  make([]*core.Transaction, 0),
		AddrSend:  make(map[uint64][]string, 0),
		StateSend: make(map[uint64][]*core.AccountState, 0),
	}
}

func (ms *MigrateSupport) ResetMigrateSupport() {
	ms.TxsSend = make(map[uint64][]*core.Transaction, 0)
	ms.TxsCross = make([]*core.Transaction, 0)
	ms.AddrSend = make(map[uint64][]string, 0)
	ms.StateSend = make(map[uint64][]*core.AccountState, 0)
}
