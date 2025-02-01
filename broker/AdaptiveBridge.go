package broker

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
)

// ABridges 是一个账号account，存在多个分片之中

type ABridges struct {
	AdaptiveBridge   core.Bridge
	ABridgesRawMegs  map[string]*message.ABridgesRawMeg // map: hash(*message.ABridgesRawMeg) → *message.ABridgesRawMeg
	ChainConfig      *params.ChainConfig
	RawTx2ABridgesTx map[string][]string
}

func (dbs *ABridges) NewABridges(pcc *params.ChainConfig) {
	dbs.ABridgesRawMegs = make(map[string]*message.ABridgesRawMeg)
	dbs.RawTx2ABridgesTx = make(map[string][]string)
	dbs.ChainConfig = pcc
	dbs.AdaptiveBridge.NewBridge()
}
