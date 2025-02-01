package message

import (
	"blockEmulator/core"
	"bytes"
	"encoding/gob"
	"log"
)

var (
	A2C_MixUpdateMsg      MessageType = "A2C_MixUpdateMsg"
	C2C_MixUpdateTxMsg    MessageType = "C2C_MixUpdateTxMsg"
	C2C_MixUpdateReady    MessageType = "C2C_MixUpdateReady"
	C2A_MixUpdateReplyMsg MessageType = "C2A_MixUpdateReplyMsg"

	MixUpdateReq RequestType = "MixUpdateReq"
)

// -------------- split
type A2C_MixUpdateMsgStruct struct {
	PartitionModified map[string]uint64
	NewSingleBroker   *core.SingleBroker
	IsInit            bool
}

func (uaq *A2C_MixUpdateMsgStruct) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(uaq)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}
func DecodeABrokersMsg(content []byte) *A2C_MixUpdateMsgStruct {
	var uaq A2C_MixUpdateMsgStruct

	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&uaq)
	if err != nil {
		log.Panic(err)
	}

	return &uaq
}

// -------------- split
type AccountMixUpdateMsg struct {
	SingleBroker *core.SingleBroker
	PartitionMap map[string]uint64

	Addrs        []string
	AccountState []*core.AccountState
	ATid         uint64
}

// -------------- split
type MixUpdateReady struct {
	FromShard uint64
	NowSeqID  uint64
}

func (atm *AccountMixUpdateMsg) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(atm)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeAccountMixUpdateMsg(content []byte) *AccountMixUpdateMsg {
	var atm AccountMixUpdateMsg
	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&atm)
	if err != nil {
		log.Panic(err)
	}
	return &atm
}

type MixUpdateInfoMsg struct {
	FromShard     uint64
	MixUpdateInfo []MixUpdateInfoStruct
}
type MixUpdateInfoStruct struct {
	SenderAddress string
	SenderShardID uint64
	AssoShard     map[uint64]bool
	Index         int
	// 添加其他许可信息
}

// this message used in inter-shard, it will be sent between leaders.
type AccountStateAndTx struct {
	Addrs        []string
	AccountState []*core.AccountState
	//delState     []*core.AccountState
	Txs       []*core.Transaction
	FromShard uint64
}
