package broker

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
)

// ABrokers 是一个账号account，存在多个分片之中

type ABrokers struct {
	AdaptiveBroker   core.Broker
	ABrokersRawMegs  map[string]*message.ABrokersRawMeg // map: hash(*message.ABrokersRawMeg) → *message.ABrokersRawMeg
	ChainConfig      *params.ChainConfig
	RawTx2ABrokersTx map[string][]string
}

func (dbs *ABrokers) NewABrokers(pcc *params.ChainConfig) {
	dbs.ABrokersRawMegs = make(map[string]*message.ABrokersRawMeg)
	dbs.RawTx2ABrokersTx = make(map[string][]string)
	dbs.ChainConfig = pcc
	dbs.AdaptiveBroker.NewBroker()
}
