package core

import (
	"blockEmulator/params"
	"blockEmulator/utils"
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type SingleBroker struct {
	AddressQue  []string
	AllocateMap map[string]map[uint64]bool // Can be stored in multiple shards
}

func CreateSingleBroker() *SingleBroker {
	SBroker := &SingleBroker{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	return SBroker
}

func (bs *SingleBroker) CheckBroker() {
	if len(bs.AddressQue) != len(bs.AllocateMap) {
		log.Panic("blockchain.CheckBroker: ActiveBrokerQue - The length of AddressQue and AllocateMap do not match")
	}
	for _, s := range bs.AllocateMap {
		if len(s) == 1 {
			log.Panic("blockchain.CheckBroker: ActiveBroker - partition in a single shard")
		}
	}
}

type Broker struct {
	ActiveBroker     *SingleBroker
	IdleBroker       *SingleBroker
	ActiveBrokerLock sync.Mutex
	IdleBrokerLock   sync.Mutex
}

func (bs *Broker) NewBroker() {
	bs.ActiveBroker = &SingleBroker{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	bs.IdleBroker = &SingleBroker{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
}

func (bs *Broker) CheckBroker() {
	bs.ActiveBroker.CheckBroker()
	bs.IdleBroker.CheckBroker()
}

func CopyBroker(bridge *Broker) *Broker {
	copyBroker := new(Broker)
	copyBroker.NewBroker()
	for _, addr := range bridge.ActiveBroker.AddressQue {
		copyBroker.ActiveBroker.AddressQue = append(copyBroker.ActiveBroker.AddressQue, addr)
	}
	for addr, shardMap := range bridge.ActiveBroker.AllocateMap {
		tempMap := make(map[uint64]bool)
		for s, b := range shardMap {
			tempMap[s] = b
		}
		copyBroker.ActiveBroker.AllocateMap[addr] = tempMap
	}

	for _, addr := range bridge.IdleBroker.AddressQue {
		copyBroker.IdleBroker.AddressQue = append(copyBroker.IdleBroker.AddressQue, addr)
	}
	for addr, shardMap := range bridge.IdleBroker.AllocateMap {
		tempMap := make(map[uint64]bool)
		for s, b := range shardMap {
			tempMap[s] = b
		}
		copyBroker.IdleBroker.AllocateMap[addr] = tempMap
	}
	return copyBroker
}

func (bs *Broker) GetActiveBrokerMap(address string) (map[uint64]bool, bool) {
	bs.ActiveBrokerLock.Lock()
	defer bs.ActiveBrokerLock.Unlock()
	ab, ok := bs.ActiveBroker.AllocateMap[address]
	return ab, ok
}

func (bs *Broker) GetIdleBrokerMap(address string) (map[uint64]bool, bool) {
	bs.IdleBrokerLock.Lock()
	defer bs.IdleBrokerLock.Unlock()
	ab, ok := bs.IdleBroker.AllocateMap[address]
	return ab, ok
}

func (bs *Broker) SetActiveBrokerMap(address string, mmap map[uint64]bool) {
	bs.ActiveBrokerLock.Lock()
	defer bs.ActiveBrokerLock.Unlock()
	bs.ActiveBroker.AllocateMap[address] = mmap
}

func (bs *Broker) SetIdleBrokerMap(address string, mmap map[uint64]bool) {
	bs.IdleBrokerLock.Lock()
	defer bs.IdleBrokerLock.Unlock()
	bs.IdleBroker.AllocateMap[address] = mmap
}

func (bs *Broker) DelActiveBrokerMap(addr string) {
	bs.ActiveBrokerLock.Lock()
	defer bs.ActiveBrokerLock.Unlock()
	delete(bs.ActiveBroker.AllocateMap, addr)
}

func (bs *Broker) DelIdleBrokerMap(addr string) {
	bs.IdleBrokerLock.Lock()
	defer bs.IdleBrokerLock.Unlock()
	delete(bs.IdleBroker.AllocateMap, addr)
}

func (bs *Broker) IsBroker(address string) int {
	if _, ok := bs.GetActiveBrokerMap(address); ok {
		return 1
	}
	if _, ok := bs.GetIdleBrokerMap(address); ok {
		return 0
	}
	return -1
}

func (bs *Broker) GetBrokerShardList(addr utils.Address) (int, []uint64) {
	isBroker := bs.IsBroker(addr)
	shardList := []uint64{}
	if isBroker == 1 {
		aam, _ := bs.GetActiveBrokerMap(addr)
		_, shardList = utils.GetMapShard(aam)
	} else if isBroker == 0 {
		Iam, _ := bs.GetIdleBrokerMap(addr)
		_, shardList = utils.GetMapShard(Iam)
	}
	return isBroker, shardList
}

func NewestThan(addr1, addr2 string, ActiveBroker, IdleBroker *SingleBroker) int { // The former at the end of the queue (longer life cycle), return 1
	_, act1 := ActiveBroker.AllocateMap[addr1]
	_, act2 := ActiveBroker.AllocateMap[addr2]
	_, idle1 := IdleBroker.AllocateMap[addr1]
	_, idle2 := IdleBroker.AllocateMap[addr2]
	exp1 := !act1 && !idle1
	exp2 := !act2 && !idle2

	if act1 && (idle2 || exp2) {
		return 1
	} else if (idle1 || exp1) && act2 {
		return -1
	} else if act1 && act2 {
		for _, addr := range ActiveBroker.AddressQue {
			if addr == addr1 {
				return 1
			}
			if addr == addr2 {
				return -1
			}
		}
	} else if idle1 && idle2 {
		for _, addr := range IdleBroker.AddressQue {
			if addr == addr1 {
				return 1
			}
			if addr == addr2 {
				return -1
			}
		}
	}
	return 0
}

func (bs *Broker) FindIntersectionBroker(shard1, shard2 uint64, NewActiveBroker *SingleBroker) string {
	num := params.BrokerNum / 2
	if params.GlobalABrokers {
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(num)
		if len(bs.ActiveBroker.AddressQue)-1 < r {
			r = len(bs.ActiveBroker.AddressQue) - 1
		}
		return bs.ActiveBroker.AddressQue[r]
	}
	count := 0
	addrList := make([]string, 0)
	for _, addr := range bs.ActiveBroker.AddressQue {
		if len(NewActiveBroker.AddressQue) != 0 {
			if _, ok := NewActiveBroker.AllocateMap[addr]; !ok {
				continue
			}
		}

		shardMap, _ := bs.GetActiveBrokerMap(addr)
		count = 0
		for i, _ := range shardMap {
			if i == shard1 {
				count++
			}
			if i == shard2 {
				count++
			}
		}
		if count == 2 {
			addrList = append(addrList, addr)
			if len(addrList) == num {
				break
			}
		}
	}
	if len(addrList) > 0 {
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(len(addrList))
		return addrList[r]
	}
	log.Panic("No Intersection core.Broker3")
	return "empty"
}

func (bs *Broker) UpdateBrokerMap(addABs *SingleBroker) *SingleBroker {
	log.Println("core.bridge：before check：", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue), "+", len(addABs.AddressQue))
	for _, newAddr := range addABs.AddressQue {
		exist := false
		for i, addr := range bs.IdleBroker.AddressQue { //
			if newAddr == addr { // idle broker is re-selected as active broker
				bs.IdleBroker.AddressQue = append(bs.IdleBroker.AddressQue[:i], bs.IdleBroker.AddressQue[i+1:]...)
				bs.DelIdleBrokerMap(addr)
				exist = true // If it's idle, it can't be active
				break
			}
		}
		if !exist {
			for i, addr := range bs.ActiveBroker.AddressQue {
				if newAddr == addr { //
					bs.ActiveBroker.AddressQue = append(bs.ActiveBroker.AddressQue[:i], bs.ActiveBroker.AddressQue[i+1:]...)
					break
				}
			}
		}
	}
	log.Println("core.bridge：after check：", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue), "+", len(addABs.AddressQue))
	for _, addr := range addABs.AddressQue {
		shard := addABs.AllocateMap[addr]
		if len(shard) == 1 {
			log.Panic("core.bridge：partition in a single shard")
		}
		bs.ActiveBroker.AddressQue = append(bs.ActiveBroker.AddressQue, addr)

		tempMap := make(map[uint64]bool, 0)
		for s, m := range shard {
			tempMap[s] = m
		}
		bs.SetActiveBrokerMap(addr, tempMap)
	}
	log.Println("core.bridge：after add：", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue))

	if len(bs.ActiveBroker.AddressQue) > params.BrokerNum {
		idx := len(bs.ActiveBroker.AddressQue) - params.BrokerNum
		bs.IdleBroker.AddressQue = append(bs.IdleBroker.AddressQue, bs.ActiveBroker.AddressQue[:idx]...)

		for _, addr := range bs.ActiveBroker.AddressQue[:idx] {
			tempMap := make(map[uint64]bool, 0)
			aam, _ := bs.GetActiveBrokerMap(addr)
			for s, m := range aam {
				tempMap[s] = m
			}
			bs.SetIdleBrokerMap(addr, tempMap)
			bs.DelActiveBrokerMap(addr)
		}

		bs.ActiveBroker.AddressQue = bs.ActiveBroker.AddressQue[idx:]
		log.Printf("core.bridge：adust idle Broker,[%d,%d]", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue))
	}
	delBrokerAddr := &SingleBroker{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	if len(bs.IdleBroker.AddressQue) > params.IdleBrokerNum {
		idx := len(bs.IdleBroker.AddressQue) - params.IdleBrokerNum

		for _, addr := range bs.IdleBroker.AddressQue[:idx] {
			delBrokerAddr.AddressQue = append(delBrokerAddr.AddressQue, addr)
			tempMap := make(map[uint64]bool, 0)
			iam, _ := bs.IdleBroker.AllocateMap[addr]
			for s, m := range iam {
				tempMap[s] = m
			}
			delBrokerAddr.AllocateMap[addr] = tempMap
			bs.DelIdleBrokerMap(addr)
		}

		bs.IdleBroker.AddressQue = bs.IdleBroker.AddressQue[idx:]
		log.Printf("core.bridge：remove expired Broker,%d", idx)
	}
	return delBrokerAddr
}

func (temp *Broker) PreUpdateBrokerMap(addABs *SingleBroker) (*SingleBroker, *SingleBroker, *SingleBroker) {
	bs := CopyBroker(temp)
	log.Println("core.bridge - PreUpdate：before check：", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue), "+", len(addABs.AddressQue))
	delBrokerAddr := &SingleBroker{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	if len(addABs.AddressQue) == 0 {
		log.Println("core.bridge - PreUpdate: no need to add new ABroker")
		return bs.ActiveBroker, bs.IdleBroker, delBrokerAddr
	}
	for _, newAddr := range addABs.AddressQue {
		exist := false
		for i, addr := range bs.IdleBroker.AddressQue {
			if newAddr == addr {
				bs.IdleBroker.AddressQue = append(bs.IdleBroker.AddressQue[:i], bs.IdleBroker.AddressQue[i+1:]...)
				delete(bs.IdleBroker.AllocateMap, addr)
				exist = true
				break
			}
		}
		if !exist {
			for i, addr := range bs.ActiveBroker.AddressQue {
				if newAddr == addr {
					bs.ActiveBroker.AddressQue = append(bs.ActiveBroker.AddressQue[:i], bs.ActiveBroker.AddressQue[i+1:]...)
					break
				}
			}
		}
	}
	log.Println("core.bridge - PreUpdate：after check：", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue), "+", len(addABs.AddressQue))
	for _, addr := range addABs.AddressQue {
		shard := addABs.AllocateMap[addr]
		if len(shard) == 1 {
			log.Panic("core.bridge：partition in a single shard")
		}
		bs.ActiveBroker.AddressQue = append(bs.ActiveBroker.AddressQue, addr)

		tempMap := make(map[uint64]bool, 0)
		for s, m := range shard {
			tempMap[s] = m
		}
		bs.ActiveBroker.AllocateMap[addr] = tempMap
	}
	log.Println("core.bridge - PreUpdate：after add：", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue))

	if len(bs.ActiveBroker.AddressQue) > params.BrokerNum {
		idx := len(bs.ActiveBroker.AddressQue) - params.BrokerNum
		bs.IdleBroker.AddressQue = append(bs.IdleBroker.AddressQue, bs.ActiveBroker.AddressQue[:idx]...) //从currentdb移到olddb

		for _, addr := range bs.ActiveBroker.AddressQue[:idx] {
			tempMap := make(map[uint64]bool, 0)
			for s, m := range bs.ActiveBroker.AllocateMap[addr] { // 从Active取出
				tempMap[s] = m
			}
			bs.IdleBroker.AllocateMap[addr] = tempMap // 添加到Expire
			delete(bs.ActiveBroker.AllocateMap, addr) //从active删除
		}

		bs.ActiveBroker.AddressQue = bs.ActiveBroker.AddressQue[idx:]
		log.Printf("core.bridge - PreUpdate：adjust idle Broker,[%d,%d]", len(bs.ActiveBroker.AddressQue), len(bs.IdleBroker.AddressQue))
	}

	if len(bs.IdleBroker.AddressQue) > params.IdleBrokerNum {
		idx := len(bs.IdleBroker.AddressQue) - params.IdleBrokerNum

		for _, addr := range bs.IdleBroker.AddressQue[:idx] {
			delBrokerAddr.AddressQue = append(delBrokerAddr.AddressQue, addr) // 添加
			tempMap := make(map[uint64]bool, 0)
			for s, m := range bs.IdleBroker.AllocateMap[addr] {
				tempMap[s] = m
			}
			delBrokerAddr.AllocateMap[addr] = tempMap
			delete(bs.IdleBroker.AllocateMap, addr)
		}

		bs.IdleBroker.AddressQue = bs.IdleBroker.AddressQue[idx:]
		log.Printf("core.bridge - PreUpdate：remuve expired Broker,%d", idx)
	}
	return bs.ActiveBroker, bs.IdleBroker, delBrokerAddr
}

func InitBrokerAddr(num int, InitMod string) *SingleBroker {
	brokerAddress := make([]string, 0)
	filePath := `./broker/broker`
	if InitMod == "CodeAlgo" {
		filePath = "./broker/CodeAlgo_" + strconv.Itoa(params.PredSize)
	} else if InitMod == "CodeRand" {
		filePath = "./broker/CodeRand" + strconv.Itoa(params.PredSize)
	}

	readFile, err := os.Open(filePath)
	if err != nil {
		log.Panic(err)
	} else {
		fmt.Println("get broker from" + filePath)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		if num == 0 {
			break
		}
		brokerAddress = append(brokerAddress, fileScanner.Text())
		num--
	}
	readFile.Close()

	NewSingleBroker := &SingleBroker{
		AddressQue:  brokerAddress,
		AllocateMap: make(map[string]map[uint64]bool),
	}
	for _, addr := range brokerAddress {
		allShardSet := make(map[uint64]bool, 0)
		for i := 0; i < params.ShardNum; i++ {
			allShardSet[uint64(i)] = false
		}
		allShardSet[uint64(utils.Addr2Shard(addr))] = true
		NewSingleBroker.AllocateMap[addr] = allShardSet
	}
	return NewSingleBroker
}
