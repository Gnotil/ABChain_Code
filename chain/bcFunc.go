package chain

import (
	"blockEmulator/core"
	"blockEmulator/utils"
)

// 是否broker，以及分布在哪些分片
func (bc *BlockChain) Get_AllocationMap(key string) (bool, uint64) {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if _, ok := bc.AllocationMap[key]; !ok { // 这个账户原来就没有存在区块链里面
		return false, uint64(utils.Addr2Shard(key)) // false表示原来不存在
	}
	return true, bc.AllocationMap[key]
}

func (bc *BlockChain) Set_AllocationMap(key string, value uint64) {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	bc.AllocationMap[key] = value
}

// Write Partition Map
func (bc *BlockChain) Update_AllocationMap(mmap map[string]uint64) {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	for key, val := range mmap {
		bc.AllocationMap[key] = val
	}
}

// Write Broker Map
func (bc *BlockChain) Update_BrokerMap(NewSingleBroker *core.SingleBroker) *core.SingleBroker {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	delBroker := bc.ABrokers.UpdateBrokerMap(NewSingleBroker)
	return delBroker
}

// Get parition (if not exist, return default)
func (bc *BlockChain) Get_BrokerMap() core.Broker {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	return bc.ABrokers
}
