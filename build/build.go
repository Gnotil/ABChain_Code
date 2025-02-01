package build

import (
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/params"
	"blockEmulator/supervisor"
	"fmt"
	"strconv"
	"time"
)

func initConfig(nid, nnm, sid, snm uint64) *params.ChainConfig {
	params.ShardNum = int(snm)
	for i := uint64(0); i < snm; i++ {
		if _, ok := params.IPmap_nodeTable[i]; !ok {
			params.IPmap_nodeTable[i] = make(map[uint64]string)
		}
		for j := uint64(0); j < nnm; j++ {
			params.IPmap_nodeTable[i][j] = "127.0.0.1:" + strconv.Itoa(28800+int(i)*100+int(j))
		}
	}
	params.IPmap_nodeTable[params.DeciderShard] = make(map[uint64]string)
	params.IPmap_nodeTable[params.DeciderShard][0] = params.SupervisorAddr // DeciderShard的0号节点就是Supervisor
	params.NodesInShard = int(nnm)
	params.ShardNum = int(snm)

	pcc := &params.ChainConfig{
		ChainID:        sid,
		NodeID:         nid,
		ShardID:        sid,
		Nodes_perShard: uint64(params.NodesInShard),
		ShardNums:      snm,
		BlockSize:      uint64(params.MaxBlockSize_global),
		BlockInterval:  uint64(params.Block_Interval),
		InjectSpeed:    uint64(params.InjectSpeed),
	}
	return pcc
}

func BuildSupervisor(nnm, snm, mod uint64) {
	fmt.Println(mod)

	lsn := new(supervisor.Supervisor)
	lsn.NewSupervisor(params.SupervisorAddr, initConfig(123, nnm, 123, snm), params.Committee, params.MeasureMethod...)
	time.Sleep(10000 * time.Millisecond)
	go lsn.SupervisorTxHandling()
	lsn.TcpListen()
}

func BuildNewPbftNode(nid, nnm, sid, snm, mod uint64) {
	worker := pbft_all.NewPbftNode(sid, nid, initConfig(nid, nnm, sid, snm), params.Committee)
	if nid == 0 {
		go worker.Propose()
		worker.TcpListen()
	} else {
		worker.TcpListen()
	}
}
