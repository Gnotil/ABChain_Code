package measure

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/utils"
)

// to test cross-transaction rate
type TestLoadBalance_Broker struct {
	epochID         int
	epochShardTxNum [][]int // epoch → <shard, txNum>
	totShardTxNum   []int   // <shard, txNum>
}

func NewTestLoadBalance_Broker() *TestLoadBalance_Broker {
	return &TestLoadBalance_Broker{
		epochID:         -1,
		epochShardTxNum: make([][]int, 0),
		totShardTxNum:   make([]int, params.ShardNum),
	}
}

func (tctr *TestLoadBalance_Broker) OutputMetricName() string {
	return "LoadBalance"
}

func (tctr *TestLoadBalance_Broker) UpdateMeasureRecord(b *message.BlockInfoMsg) {}

func (tctr *TestLoadBalance_Broker) UpdateMeasureRecordWithSet(b *message.BlockInfoMsg, shardLoad []int) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}
	epochid := b.Epoch
	// extend
	for tctr.epochID < epochid {
		tctr.epochID++
		tctr.epochShardTxNum = append(tctr.epochShardTxNum, make([]int, len(shardLoad)))
	}
	for i, item := range shardLoad {
		tctr.epochShardTxNum[epochid][i] += item // 累加到当前epoch
		tctr.totShardTxNum[i] += item            // 直接累加到总数
	}
}

func (tctr *TestLoadBalance_Broker) HandleExtraMessage([]byte) {}

func (tctr *TestLoadBalance_Broker) OutputRecord() (perEpochVariance []float64, perEpochTotVariance float64) {
	perEpochLoadBalance := make([]float64, 0)
	perEpochLoadBalanceAvg := 0.0
	for _, epochTx := range tctr.epochShardTxNum {
		_, _, VariantCoe := utils.Variance(epochTx)
		perEpochLoadBalance = append(perEpochLoadBalance, VariantCoe)
		perEpochLoadBalanceAvg += VariantCoe
	}
	perEpochLoadBalanceAvg /= float64(len(tctr.epochShardTxNum))
	perEpochLoadBalance = append(perEpochLoadBalance, perEpochLoadBalanceAvg)
	_, _, totLoadBalance := utils.Variance(tctr.totShardTxNum)
	return perEpochLoadBalance, totLoadBalance
}
