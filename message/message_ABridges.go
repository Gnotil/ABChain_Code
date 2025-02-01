package message

import (
	"blockEmulator/core"
	"blockEmulator/utils"
)

var (
	CrInner2CrossTx MessageType = "innerShardTx_be_crossShard"
)

type ABridgesRawMeg struct {
	Tx        *core.Transaction
	Bridge    utils.Address
	Hlock     uint64 //ignore
	Snonce    uint64 //ignore
	Bnonce    uint64 //ignore
	Signature []byte // not implemented now.

	RootTxHash []byte
}

type ABridgesType1Meg struct {
	RawMeg   *ABridgesRawMeg
	Hcurrent uint64        //ignore
	Bridge   utils.Address // replace signature of bridge
}

type ABridgesMag1Confirm struct {
	//RawDigest string
	Tx1Hash []byte
	RawMeg  *ABridgesRawMeg
}

type ABridgesType2Meg struct {
	RawMeg *ABridgesRawMeg
	Bridge utils.Address // replace signature of bridge
}

type ABridgesMag2Confirm struct {
	//RawDigest string
	Tx2Hash []byte
	RawMeg  *ABridgesRawMeg
}

type ABridgesTxMap struct {
	ABridgesTx2ABridges12 map[string][]string // map: raw bridge tx to its bridge1Tx and bridge2Tx
}

type InnerTx2CrossTx struct {
	FromShard uint64
	Txs       []*core.Transaction // if an inner-shard tx becomes a cross-shard tx, it will be added into here.
}
