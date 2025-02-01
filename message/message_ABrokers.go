package message

import (
	"blockEmulator/core"
	"blockEmulator/utils"
)

var (
	CrInner2CrossTx MessageType = "innerShardTx_be_crossShard"
)

type ABrokersRawMeg struct {
	Tx        *core.Transaction
	Broker    utils.Address
	Hlock     uint64 //ignore
	Snonce    uint64 //ignore
	Bnonce    uint64 //ignore
	Signature []byte // not implemented now.

	RootTxHash []byte
}

type ABrokersType1Meg struct {
	RawMeg   *ABrokersRawMeg
	Hcurrent uint64        //ignore
	Broker   utils.Address // replace signature of bridge
}

type ABrokersMag1Confirm struct {
	//RawDigest string
	Tx1Hash []byte
	RawMeg  *ABrokersRawMeg
}

type ABrokersType2Meg struct {
	RawMeg *ABrokersRawMeg
	Broker utils.Address // replace signature of bridge
}

type ABrokersMag2Confirm struct {
	//RawDigest string
	Tx2Hash []byte
	RawMeg  *ABrokersRawMeg
}

type ABrokersTxMap struct {
	ABrokersTx2ABrokers12 map[string][]string // map: raw bridge tx to its bridge1Tx and bridge2Tx
}

type InnerTx2CrossTx struct {
	FromShard uint64
	Txs       []*core.Transaction // if an inner-shard tx becomes a cross-shard tx, it will be added into here.
}
