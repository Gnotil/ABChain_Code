package measure

import (
	"blockEmulator/message"
	"blockEmulator/params"
	_ "blockEmulator/params"
	"strconv"
	"time"
)

type TestModule_avgTPS_Broker struct {
	epochID      int
	excutedTxNum []float64 // record how many excuted txs in an epoch, maybe the cross shard tx will be calculated as a 0.5 tx

	broker1TxNum []int // record how many broker1 txs in an epoch.
	broker2TxNum []int // record how many broker2 txs in an epoch.
	normalTxNum  []int // record how many normal txs in an epoch.

	startTime []time.Time // record when the epoch starts
	endTime   []time.Time // record when the epoch ends
}

func NewTestModule_avgTPS_Broker() *TestModule_avgTPS_Broker {
	return &TestModule_avgTPS_Broker{
		epochID:      -1,
		excutedTxNum: make([]float64, 0),
		startTime:    make([]time.Time, 0),
		endTime:      make([]time.Time, 0),

		broker1TxNum: make([]int, 0),
		broker2TxNum: make([]int, 0),
		normalTxNum:  make([]int, 0),
	}
}

func (tat *TestModule_avgTPS_Broker) OutputMetricName() string {
	return "Average_TPS"
}

// add the number of excuted txs, and change the time records
func (tat *TestModule_avgTPS_Broker) UpdateMeasureRecord(bim *message.BlockInfoMsg) {
	if bim.BlockBodyLength == 0 { // empty block
		return
	}

	epochid := bim.Epoch
	innerShardTxNum := len(bim.InnerShardTxs)
	earliestTime := bim.ProposeTime
	latestTime := bim.CommitTime
	b1TxNum := len(bim.Broker1Txs)
	b2TxNum := len(bim.Broker2Txs)

	// extend
	for tat.epochID < epochid {
		tat.excutedTxNum = append(tat.excutedTxNum, 0)

		tat.startTime = append(tat.startTime, time.Time{})
		tat.endTime = append(tat.endTime, time.Time{})

		tat.broker1TxNum = append(tat.broker1TxNum, 0)
		tat.broker2TxNum = append(tat.broker2TxNum, 0)
		tat.normalTxNum = append(tat.normalTxNum, 0)

		tat.epochID++
	}

	// modify the local epoch
	tat.excutedTxNum[epochid] += float64(innerShardTxNum) + (float64(b1TxNum+b2TxNum))/2 // ================= 统计总交易数
	tat.broker1TxNum[epochid] += b1TxNum
	tat.broker2TxNum[epochid] += b2TxNum
	tat.normalTxNum[epochid] += innerShardTxNum

	// modify time
	if tat.startTime[epochid].IsZero() || tat.startTime[epochid].After(earliestTime) {
		tat.startTime[epochid] = earliestTime
	}

	if tat.endTime[epochid].IsZero() || latestTime.After(tat.endTime[epochid]) {
		tat.endTime[epochid] = latestTime
	}
}

func (tat *TestModule_avgTPS_Broker) HandleExtraMessage([]byte) {}

// output the average TPS
func (tat *TestModule_avgTPS_Broker) OutputRecord() (perEpochTPS []float64, totalTPS float64) {
	// write details to csv
	tat.writeToCSV()

	// calculate the simple result
	perEpochTPS = make([]float64, tat.epochID+1)
	totalTxNum := 0.0
	eTime := time.Now()
	lTime := time.Time{}
	for eid, exTxNum := range tat.excutedTxNum {
		timeGap := tat.endTime[eid].Sub(tat.startTime[eid]).Seconds()
		perEpochTPS[eid] = exTxNum / timeGap
		totalTxNum += exTxNum
		if eTime.After(tat.startTime[eid]) {
			eTime = tat.startTime[eid]
		}
		if tat.endTime[eid].After(lTime) {
			lTime = tat.endTime[eid]
		}
	}
	totalTPS = totalTxNum / (lTime.Sub(eTime).Seconds())
	return
}

func (tat *TestModule_avgTPS_Broker) writeToCSV() {
	fileName := tat.OutputMetricName()
	measureName := []string{"EpochID", "Total tx # in this epoch", "Normal tx # in this epoch", "Broker1 tx # in this epoch",
		"Broker2 tx # in this epoch", "Epoch start time", "Epoch end time", "Spent time", "Avg. TPS of this epoch", "Tot time", "Tot Avg. TPS"}
	measureVals := make([][]string, 0)

	timeGapSum := 0.0
	for eid, exTxNum := range tat.excutedTxNum {
		timeGap := tat.endTime[eid].Sub(tat.startTime[eid]).Seconds()
		timeGapSum += timeGap
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.FormatFloat(exTxNum, 'f', '8', 64),
			strconv.Itoa(tat.normalTxNum[eid]),
			strconv.Itoa(tat.broker1TxNum[eid]),
			strconv.Itoa(tat.broker2TxNum[eid]),
			strconv.Quote(tat.startTime[eid].String()),
			strconv.Quote(tat.endTime[eid].String()),
			//strconv.FormatInt(tat.startTime[eid].UnixMilli(), 10),
			//strconv.FormatInt(tat.endTime[eid].UnixMilli(), 10),
			strconv.FormatFloat(timeGap, 'f', '8', 64),
			strconv.FormatFloat(exTxNum/timeGap, 'f', '8', 64),
		}
		measureVals = append(measureVals, csvLine)
	}
	// 写一个sum
	totTimeGap := tat.endTime[len(tat.endTime)-1].Sub(tat.startTime[0]).Seconds()
	sum_csvLine := []string{
		strconv.Itoa(999),
		strconv.FormatFloat(FloatArraySum(tat.excutedTxNum), 'f', '8', 64),
		strconv.Itoa(IntArraySum(tat.normalTxNum)),
		strconv.Itoa(IntArraySum(tat.broker1TxNum)),
		strconv.Itoa(IntArraySum(tat.broker2TxNum)),
		strconv.Quote(tat.startTime[0].String()),
		strconv.Quote(tat.endTime[len(tat.endTime)-1].String()),
		//strconv.FormatInt(tat.startTime[1].UnixMilli(), 10),
		//strconv.FormatInt(tat.endTime[len(tat.endTime)-1].UnixMilli(), 10),
		strconv.FormatFloat(timeGapSum, 'f', '8', 64),
		strconv.FormatFloat(FloatArraySum(tat.excutedTxNum)/timeGapSum, 'f', '8', 64),

		strconv.FormatFloat(totTimeGap, 'f', '8', 64),
		strconv.FormatFloat(float64(params.TotalDataSize)/totTimeGap, 'f', '8', 64),
	}
	measureVals = append(measureVals, sum_csvLine)

	WriteMetricsToCSV(fileName, measureName, measureVals)
}
