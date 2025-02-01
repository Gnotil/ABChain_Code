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

//type AssoShard struct {
//	mainShard uint64
//	shardList map[uint64]bool
//}

type SingleBridge struct {
	AddressQue  []string
	AllocateMap map[string]map[uint64]bool // 可以存储在多个分片
}

func CreateSingleBridge() *SingleBridge {
	SBridge := &SingleBridge{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	return SBridge
}

func (bs *SingleBridge) CheckBridge() {
	if len(bs.AddressQue) != len(bs.AllocateMap) {
		log.Panic("blockchain.CheckBridge: ActiveBridgeQue - AddressQue与AllocateMap长度不匹配")
	}
	for _, s := range bs.AllocateMap {
		if len(s) == 1 {
			log.Panic("blockchain.CheckBridge: ActiveBridge - 分布在单一分片")
		}
	}
}

type Bridge struct {
	ActiveBridge     *SingleBridge
	IdleBridge       *SingleBridge
	ActiveBridgeLock sync.Mutex
	IdleBridgeLock   sync.Mutex
}

func (bs *Bridge) NewBridge() {
	bs.ActiveBridge = &SingleBridge{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	bs.IdleBridge = &SingleBridge{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
}

func (bs *Bridge) CheckBridge() {
	bs.ActiveBridge.CheckBridge()
	bs.IdleBridge.CheckBridge()
}

func CopyBridge(bridge *Bridge) *Bridge {
	copyBridge := new(Bridge)
	copyBridge.NewBridge()
	for _, addr := range bridge.ActiveBridge.AddressQue {
		copyBridge.ActiveBridge.AddressQue = append(copyBridge.ActiveBridge.AddressQue, addr)
	}
	for addr, shardMap := range bridge.ActiveBridge.AllocateMap {
		tempMap := make(map[uint64]bool)
		for s, b := range shardMap {
			tempMap[s] = b
		}
		copyBridge.ActiveBridge.AllocateMap[addr] = tempMap
	}

	for _, addr := range bridge.IdleBridge.AddressQue {
		copyBridge.IdleBridge.AddressQue = append(copyBridge.IdleBridge.AddressQue, addr)
	}
	for addr, shardMap := range bridge.IdleBridge.AllocateMap {
		tempMap := make(map[uint64]bool)
		for s, b := range shardMap {
			tempMap[s] = b
		}
		copyBridge.IdleBridge.AllocateMap[addr] = tempMap
	}
	return copyBridge
}

func (bs *Bridge) GetActiveBridgeMap(address string) (map[uint64]bool, bool) {
	bs.ActiveBridgeLock.Lock()
	defer bs.ActiveBridgeLock.Unlock()
	ab, ok := bs.ActiveBridge.AllocateMap[address]
	return ab, ok
}

func (bs *Bridge) GetIdleBridgeMap(address string) (map[uint64]bool, bool) {
	bs.IdleBridgeLock.Lock()
	defer bs.IdleBridgeLock.Unlock()
	ab, ok := bs.IdleBridge.AllocateMap[address]
	return ab, ok
}

func (bs *Bridge) SetActiveBridgeMap(address string, mmap map[uint64]bool) {
	bs.ActiveBridgeLock.Lock()
	defer bs.ActiveBridgeLock.Unlock()
	bs.ActiveBridge.AllocateMap[address] = mmap
}

func (bs *Bridge) SetIdleBridgeMap(address string, mmap map[uint64]bool) {
	bs.IdleBridgeLock.Lock()
	defer bs.IdleBridgeLock.Unlock()
	bs.IdleBridge.AllocateMap[address] = mmap
}

func (bs *Bridge) DelActiveBridgeMap(addr string) {
	bs.ActiveBridgeLock.Lock()
	defer bs.ActiveBridgeLock.Unlock()
	delete(bs.ActiveBridge.AllocateMap, addr)
}

func (bs *Bridge) DelIdleBridgeMap(addr string) {
	bs.IdleBridgeLock.Lock()
	defer bs.IdleBridgeLock.Unlock()
	delete(bs.IdleBridge.AllocateMap, addr)
}

func (bs *Bridge) IsBridge(address string) int {
	if _, ok := bs.GetActiveBridgeMap(address); ok {
		return 1
	}
	if _, ok := bs.GetIdleBridgeMap(address); ok {
		return 0
	}
	return -1
}

func (bs *Bridge) GetBridgeShardList(addr utils.Address) (int, []uint64) {
	isBridge := bs.IsBridge(addr)
	shardList := []uint64{}
	if isBridge == 1 {
		aam, _ := bs.GetActiveBridgeMap(addr)
		_, shardList = utils.GetMapShard(aam)
	} else if isBridge == 0 {
		Iam, _ := bs.GetIdleBridgeMap(addr)
		_, shardList = utils.GetMapShard(Iam)
	}
	return isBridge, shardList
}

func NewestThan(addr1, addr2 string, ActiveBridge, IdleBridge *SingleBridge) int { // 前面先，为1
	_, act1 := ActiveBridge.AllocateMap[addr1]
	_, act2 := ActiveBridge.AllocateMap[addr2]
	_, idle1 := IdleBridge.AllocateMap[addr1]
	_, idle2 := IdleBridge.AllocateMap[addr2]
	exp1 := !act1 && !idle1
	exp2 := !act2 && !idle2

	if act1 && (idle2 || exp2) {
		return 1
	} else if (idle1 || exp1) && act2 {
		return -1
	} else if act1 && act2 {
		for _, addr := range ActiveBridge.AddressQue {
			if addr == addr1 {
				return 1
			}
			if addr == addr2 {
				return -1
			}
		}
	} else if idle1 && idle2 {
		for _, addr := range IdleBridge.AddressQue {
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

// func (bs *Bridge) NewestThan(addr1, addr2 string) bool { // 前面先，为1
//
//		if bs.IsBridge(addr1) > bs.IsBridge(addr2) {
//			return true
//		} else if bs.IsBridge(addr1) < bs.IsBridge(addr2) {
//			return false
//		}
//		if bs.IsBridge(addr1) == 1 {
//			for _, addr := range bs.ActiveBridge.AddressQue {
//				if addr == addr1 {
//					return true
//				}
//				if addr == addr2 {
//					return false
//				}
//			}
//		}
//		if bs.IsBridge(addr1) == 0 {
//			for _, addr := range bs.IdleBridge.AddressQue {
//				if addr == addr1 {
//					return true
//				}
//				if addr == addr2 {
//					return false
//				}
//			}
//		}
//		return false
//	}
func (bs *Bridge) FindIntersectionBridge(shard1, shard2 uint64, NewActiveBridge *SingleBridge) string {
	num := params.BrokerNum / 2
	if params.GlobalABridges {
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(num)
		if len(bs.ActiveBridge.AddressQue)-1 < r {
			r = len(bs.ActiveBridge.AddressQue) - 1
		}
		return bs.ActiveBridge.AddressQue[r]
	}
	count := 0
	addrList := make([]string, 0)
	for _, addr := range bs.ActiveBridge.AddressQue { //按顺序
		if len(NewActiveBridge.AddressQue) != 0 {
			if _, ok := NewActiveBridge.AllocateMap[addr]; !ok { // 不在ABN，跳过
				continue
			}
		}
		// 此时addr既在ActiveBridge，又在NewActiveBridge
		shardMap, _ := bs.GetActiveBridgeMap(addr)
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
	log.Panic("No Intersection core.Bridge3")
	return "empty"
}

func (bs *Bridge) UpdateBridgeMap(addABs *SingleBridge) *SingleBridge {
	log.Println("core.bridge：检查前：", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue), "+", len(addABs.AddressQue))
	for _, newAddr := range addABs.AddressQue {
		exist := false
		for i, addr := range bs.IdleBridge.AddressQue { //遍历旧
			if newAddr == addr { // 过期db重新被选为新db
				bs.IdleBridge.AddressQue = append(bs.IdleBridge.AddressQue[:i], bs.IdleBridge.AddressQue[i+1:]...) //从olddb Queue中删除
				bs.DelIdleBridgeMap(addr)
				//delete(bs.IdleBridge.AllocateMap, addr)                                                            // 从map中删除
				exist = true // 如果已经在old，那就不可能在current
				break
			}
		}
		if !exist {
			for i, addr := range bs.ActiveBridge.AddressQue { //遍历现有
				if newAddr == addr { //
					bs.ActiveBridge.AddressQue = append(bs.ActiveBridge.AddressQue[:i], bs.ActiveBridge.AddressQue[i+1:]...) // 从currentdb Queue中删除
					// 不需要从map删除，直接赋值就行,因为map没有顺序
					break
				}
			}
		}
	}
	log.Println("core.bridge：检查后：", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue), "+", len(addABs.AddressQue))
	for _, addr := range addABs.AddressQue {
		//fmt.Println("中间检查") // 这个函数的问题
		//utils.GetMapShard(shard)
		shard := addABs.AllocateMap[addr]
		if len(shard) == 1 {
			log.Panic("core.bridge：分片分布为1")
		}
		bs.ActiveBridge.AddressQue = append(bs.ActiveBridge.AddressQue, addr) // queue直接添加

		tempMap := make(map[uint64]bool, 0)
		for s, m := range shard {
			tempMap[s] = m
		}
		bs.SetActiveBridgeMap(addr, tempMap) // 直接覆盖
	}
	log.Println("core.bridge：添加后：", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue))

	if len(bs.ActiveBridge.AddressQue) > params.BrokerNum {
		idx := len(bs.ActiveBridge.AddressQue) - params.BrokerNum
		bs.IdleBridge.AddressQue = append(bs.IdleBridge.AddressQue, bs.ActiveBridge.AddressQue[:idx]...) //从currentdb移到olddb

		for _, addr := range bs.ActiveBridge.AddressQue[:idx] {
			tempMap := make(map[uint64]bool, 0)
			aam, _ := bs.GetActiveBridgeMap(addr)
			for s, m := range aam { // 从Active取出
				tempMap[s] = m
			}
			bs.SetIdleBridgeMap(addr, tempMap) // 添加到Expire
			bs.DelActiveBridgeMap(addr)
			//delete(bs.ActiveBridge.AllocateMap, addr) //从active删除
		}

		bs.ActiveBridge.AddressQue = bs.ActiveBridge.AddressQue[idx:]
		log.Printf("core.bridge：调整过时DynamicBridge,[%d,%d]", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue))
		//for _, addr := range bs.ActiveBridge.AddressQue {
		//	fmt.Println("A - ", addr)
		//}
		//for _, addr := range bs.ActiveBridge.AddressQue {
		//	fmt.Println("E - ", addr)
		//}
	}
	delBridgeAddr := &SingleBridge{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	if len(bs.IdleBridge.AddressQue) > params.IdleBridgeNum {
		idx := len(bs.IdleBridge.AddressQue) - params.IdleBridgeNum

		for _, addr := range bs.IdleBridge.AddressQue[:idx] {
			delBridgeAddr.AddressQue = append(delBridgeAddr.AddressQue, addr) // 添加
			tempMap := make(map[uint64]bool, 0)
			iam, _ := bs.IdleBridge.AllocateMap[addr]
			for s, m := range iam {
				tempMap[s] = m
			}
			delBridgeAddr.AllocateMap[addr] = tempMap
			bs.DelIdleBridgeMap(addr)
			//delete(bs.IdleBridge.AllocateMap, addr)
		}

		bs.IdleBridge.AddressQue = bs.IdleBridge.AddressQue[idx:]
		log.Printf("core.bridge：删除失效DynamicBridge,%d", idx)
	}
	return delBridgeAddr
}

func (temp *Bridge) PreUpdateBridgeMap(addABs *SingleBridge) (*SingleBridge, *SingleBridge, *SingleBridge) {
	bs := CopyBridge(temp)
	log.Println("core.bridge - PreUpdate：检查前：", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue), "+", len(addABs.AddressQue))
	delBridgeAddr := &SingleBridge{
		AddressQue:  make([]string, 0),
		AllocateMap: make(map[string]map[uint64]bool),
	}
	if len(addABs.AddressQue) == 0 {
		log.Println("core.bridge - PreUpdate: 无需添加新ABridge")
		return bs.ActiveBridge, bs.IdleBridge, delBridgeAddr
	}
	for _, newAddr := range addABs.AddressQue {
		exist := false
		for i, addr := range bs.IdleBridge.AddressQue { //遍历旧
			if newAddr == addr { // 过期db重新被选为新db
				bs.IdleBridge.AddressQue = append(bs.IdleBridge.AddressQue[:i], bs.IdleBridge.AddressQue[i+1:]...) //从olddb Queue中删除
				delete(bs.IdleBridge.AllocateMap, addr)                                                            // 从map中删除
				exist = true                                                                                       // 如果已经在old，那就不可能在current
				break
			}
		}
		if !exist {
			for i, addr := range bs.ActiveBridge.AddressQue { //遍历现有
				if newAddr == addr { //
					bs.ActiveBridge.AddressQue = append(bs.ActiveBridge.AddressQue[:i], bs.ActiveBridge.AddressQue[i+1:]...) // 从currentdb Queue中删除
					// 不需要从map删除，直接赋值就行,因为map没有顺序
					break
				}
			}
		}
	}
	log.Println("core.bridge - PreUpdate：检查后：", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue), "+", len(addABs.AddressQue))
	for _, addr := range addABs.AddressQue {
		//fmt.Println("中间检查") // 这个函数的问题
		//utils.GetMapShard(shard)
		shard := addABs.AllocateMap[addr]
		if len(shard) == 1 {
			log.Panic("core.bridge：分片分布为1")
		}
		bs.ActiveBridge.AddressQue = append(bs.ActiveBridge.AddressQue, addr) // queue直接添加

		tempMap := make(map[uint64]bool, 0)
		for s, m := range shard {
			tempMap[s] = m
		}
		bs.ActiveBridge.AllocateMap[addr] = tempMap // 直接覆盖
	}
	log.Println("core.bridge - PreUpdate：添加后：", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue))

	if len(bs.ActiveBridge.AddressQue) > params.BrokerNum {
		idx := len(bs.ActiveBridge.AddressQue) - params.BrokerNum
		bs.IdleBridge.AddressQue = append(bs.IdleBridge.AddressQue, bs.ActiveBridge.AddressQue[:idx]...) //从currentdb移到olddb

		for _, addr := range bs.ActiveBridge.AddressQue[:idx] {
			tempMap := make(map[uint64]bool, 0)
			for s, m := range bs.ActiveBridge.AllocateMap[addr] { // 从Active取出
				tempMap[s] = m
			}
			bs.IdleBridge.AllocateMap[addr] = tempMap // 添加到Expire
			delete(bs.ActiveBridge.AllocateMap, addr) //从active删除
		}

		bs.ActiveBridge.AddressQue = bs.ActiveBridge.AddressQue[idx:]
		log.Printf("core.bridge - PreUpdate：调整过时DynamicBridge,[%d,%d]", len(bs.ActiveBridge.AddressQue), len(bs.IdleBridge.AddressQue))
	}

	if len(bs.IdleBridge.AddressQue) > params.IdleBridgeNum {
		idx := len(bs.IdleBridge.AddressQue) - params.IdleBridgeNum

		for _, addr := range bs.IdleBridge.AddressQue[:idx] {
			delBridgeAddr.AddressQue = append(delBridgeAddr.AddressQue, addr) // 添加
			tempMap := make(map[uint64]bool, 0)
			for s, m := range bs.IdleBridge.AllocateMap[addr] {
				tempMap[s] = m
			}
			delBridgeAddr.AllocateMap[addr] = tempMap
			delete(bs.IdleBridge.AllocateMap, addr)
		}

		bs.IdleBridge.AddressQue = bs.IdleBridge.AddressQue[idx:]
		log.Printf("core.bridge - PreUpdate：删除失效DynamicBridge,%d", idx)
	}
	return bs.ActiveBridge, bs.IdleBridge, delBridgeAddr
}

func InitBrokerAddr(num int, InitMod string) *SingleBridge {
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
		fmt.Println("从" + filePath + "读取broker")
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

	NewSingleBridge := &SingleBridge{
		AddressQue:  brokerAddress,
		AllocateMap: make(map[string]map[uint64]bool),
	}
	for _, addr := range brokerAddress { // 遍历所有hot账户
		allShardSet := make(map[uint64]bool, 0)
		for i := 0; i < params.ShardNum; i++ {
			allShardSet[uint64(i)] = false
		}
		allShardSet[uint64(utils.Addr2Shard(addr))] = true
		NewSingleBridge.AllocateMap[addr] = allShardSet
	}
	return NewSingleBridge
}
