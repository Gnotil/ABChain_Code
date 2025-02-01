// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp controll
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule

	// diy, add more structures or classes here ...
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, mearsureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.Ss = signal.NewStopSignal(2 * int(pcc.ShardNums))

	switch committeeMethod {
	case "AdaptiveBridges":
		d.comMod = committee.NewABCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.FileInputSet, params.FileIndexBegin,
			params.FileIndexEnd, params.TotalDataSize, params.BatchSize, params.ReconfigTimeGap)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range mearsureModNames {
		switch mModName {
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		case "LoadBalance_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestLoadBalance_Broker())
		default:
		}
	}
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		//d.sl.Slog.Println("bim.BlockBodyLength =", bim.BlockBodyLength, "Gap++")
		d.Ss.StopGap_Inc()
	} else {
		//d.sl.Slog.Println("bim.BlockBodyLength =", bim.BlockBodyLength, "GapReset")
		d.Ss.StopGap_Reset()
	}
	d.comMod.HandleBlockInfo(bim)

	// measure update
	//d.sl.Slog.Println("----------------------------\n开始测试记录信息\n----------------------------")
	for _, measureMod := range d.testMeasureMods { // 遍历4种 measureMod
		measureMod.UpdateMeasureRecord(bim)

		//switch measureMod.OutputMetricName() {
		//case "LoadBalance":
		//	shardLoad := d.comMod.GetShardLoadMap()
		//	shardLoad = shardLoad[:len(shardLoad)-4]
		//	measureMod.UpdateMeasureRecordWithSet(bim, shardLoad)
		//case "Tx_number":
		//	BrokerNum := d.comMod.GetShardLoadMap()
		//	BrokerNum = []int{BrokerNum[len(BrokerNum)-4], BrokerNum[len(BrokerNum)-3], BrokerNum[len(BrokerNum)-2], BrokerNum[len(BrokerNum)-1]}
		//	measureMod.UpdateMeasureRecordWithSet(bim, BrokerNum)
		//default:
		//	measureMod.UpdateMeasureRecord(bim) // 开始记录 测试信息
		//}
		//if measureMod.OutputMetricName() == "LoadBalance" {
		//	shardLoad := d.comMod.GetShardLoadMap()
		//	measureMod.UpdateMeasureRecordWithSet(bim, shardLoad)
		//}
		//if measureMod.OutputMetricName() == "LoadBalance" {
		//	shardLoad := d.comMod.GetShardLoadMap()
		//	measureMod.UpdateMeasureRecordWithSet(bim, shardLoad)
		//} else {
		//	measureMod.UpdateMeasureRecord(bim) // 开始记录 测试信息
		//}
	}
	// add codes here ...
}

func (d *Supervisor) handleMixUpdateInfos(content []byte) {
	bim := new(message.MixUpdateInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	d.comMod.HandleMixUpdateInfo(bim)
	// add codes here ...
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	d.comMod.MsgSendingControl()

	// TxHandling is end
	//for !d.Ss.GapEnough() { // wait all txs to be handled
	//	time.Sleep(time.Second)
	//}
	// send stop message
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			networks.TcpDial(stopmsg, d.Ip_nodeTable[sid][nid])
		}
	}
	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		//fmt.Println("处理CBlockInfo")
		d.handleBlockInfos(content)
		// add codes for more functionality
	case message.C2A_MixUpdateReplyMsg:
		//fmt.Println("处理C2A_MixUpdateReplyMsg")
		d.handleMixUpdateInfos(content)
		// add codes for more functionality
	default:
		d.comMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			d.tcpLock.Lock()
			//msgType, _ := message.SplitMessage(clientRequest)
			//fmt.Println("从conn监听到:", msgType)
			d.handleMessage(clientRequest)
			d.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// tcp listen for Supervisor
func (d *Supervisor) OldTcpListen() {
	ipaddr, err := net.ResolveTCPAddr("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	ln, err := net.ListenTCP("tcp", ipaddr)
	d.tcpLn = ln
	if err != nil {
		log.Panic(err)
	}
	d.sl.Slog.Printf("Supervisor begins listening：%s\n", d.IPaddr)

	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			if d.listenStop {
				return
			}
			log.Panic(err)
		}
		b, err := io.ReadAll(conn)
		if err != nil {
			log.Panic(err)
		}
		d.handleMessage(b)
		conn.(*net.TCPConn).SetLinger(0)
		defer conn.Close()
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}
