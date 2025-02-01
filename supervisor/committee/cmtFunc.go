package committee

import (
	"blockEmulator/core"
	"blockEmulator/utils"
	"log"
	"math/big"
)

type Pair struct {
	Key   string
	Value int
}

func data2tx(data []string, nonce uint64) (*core.Transaction, bool) {
	if data[6] == "0" && data[7] == "0" && len(data[3]) > 16 && len(data[4]) > 16 && data[3] != data[4] {
		val, ok := new(big.Int).SetString(data[8], 10)
		if !ok {
			log.Panic("new int failed\n")
		}
		tx := core.NewTransaction(data[3][2:], data[4][2:], val, nonce)
		return tx, true
	}
	return &core.Transaction{}, false
}

func (ccm *ABCommitteeMod) GetAllocationMap(addr utils.Address) (bool, uint64) {
	ccm.alloLock.RLock()
	defer ccm.alloLock.RUnlock()
	if _, ok := ccm.AllocationMap[addr]; !ok { // 这个账户原来就没有存在区块链里面
		return false, uint64(utils.Addr2Shard(addr)) // false表示原来不存在
	}
	return true, ccm.AllocationMap[addr]
}

func (ccm *ABCommitteeMod) UpdateAllocationMap(mmap map[string]uint64) {
	ccm.alloLock.Lock()
	defer ccm.alloLock.Unlock()
	for key, val := range mmap {
		ccm.AllocationMap[key] = val
	}
}

func (ccm *ABCommitteeMod) IsBridge(addr utils.Address) int {
	ccm.abridgeLock.RLock()
	result := ccm.ABridges.AdaptiveBridge.IsBridge(addr)
	ccm.abridgeLock.RUnlock()
	return result
}

func (ccm *ABCommitteeMod) getSafeAllo(addr string) (uint64, []uint64) {
	_, mainShard := ccm.GetAllocationMap(addr)
	shardList := []uint64{mainShard}
	if len(ccm.NewActiveBridge.AddressQue) != 0 {
		if _, ok1 := ccm.NewActiveBridge.AllocateMap[addr]; ok1 {
			_, shardList = utils.GetMapShard(ccm.NewActiveBridge.AllocateMap[addr])
		} else if _, ok2 := ccm.NewIdleBridge.AllocateMap[addr]; ok2 {
			_, shardList = utils.GetMapShard(ccm.NewIdleBridge.AllocateMap[addr])
		} else {
			shardList = []uint64{mainShard}
		}
	}
	return mainShard, shardList
}

func (ccm *ABCommitteeMod) getNewAllo(addr string) (uint64, []uint64) {
	_, mainShard := ccm.GetAllocationMap(addr)
	isBridge, shardList := ccm.ABridges.AdaptiveBridge.GetBridgeShardList(addr)
	if isBridge == -1 {
		shardList = []uint64{mainShard}
	}

	if len(ccm.NewActiveBridge.AllocateMap) != 0 {
		if _, ok1 := ccm.NewActiveBridge.AllocateMap[addr]; ok1 {
			_, shardList = utils.GetMapShard(ccm.NewActiveBridge.AllocateMap[addr])
		} else if _, ok2 := ccm.NewIdleBridge.AllocateMap[addr]; ok2 {
			_, shardList = utils.GetMapShard(ccm.NewIdleBridge.AllocateMap[addr])
		} else {
			shardList = []uint64{mainShard}
		}
	}
	return mainShard, shardList
}

func (ccm *ABCommitteeMod) getActiveAllo(addr string) (uint64, []uint64) {
	_, mainShard := ccm.GetAllocationMap(addr)
	isBridge, shardList := ccm.ABridges.AdaptiveBridge.GetBridgeShardList(addr)
	if isBridge == -1 || isBridge == 0 { //isBridge==0，说明是idle broker，当做普通账户来处理
		shardList = []uint64{mainShard}
	}

	if len(ccm.NewActiveBridge.AllocateMap) != 0 {
		if _, ok1 := ccm.NewActiveBridge.AllocateMap[addr]; ok1 {
			_, shardList = utils.GetMapShard(ccm.NewActiveBridge.AllocateMap[addr])
		}
	}

	return mainShard, shardList
}
