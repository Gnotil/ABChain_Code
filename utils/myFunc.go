package utils

import (
	"blockEmulator/params"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"
)

type Address = string

// the default method
func Addr2Shard(addr Address) int {
	last16_addr := addr[len(addr)-8:]
	num, err := strconv.ParseUint(last16_addr, 16, 64)
	if err != nil {
		log.Panic(err)
	}
	return int(num) % params.ShardNum
}

/*
	================================================= my func
*/

func Variance(input []int) (float64, float64, float64) {
	data := make([]float64, len(input))
	for i, d := range input {
		data[i] = float64(d)
	}
	n := len(data)
	if n <= 1 {
		return 0.0, 0.0, 0.0
	}

	sum := 0.0
	for _, x := range data {
		sum += x
	}
	mean := sum / float64(n)

	sumOfSquares := 0.0
	for _, x := range data {
		diff := x - mean
		sumOfSquares += diff * diff
	}

	variance := sumOfSquares / float64(n-1)
	//Standard Deviation
	StandardDeviation := math.Sqrt(variance)
	VariantCoe := StandardDeviation / mean
	return variance, StandardDeviation, VariantCoe
}

func InIntList(i uint64, list []uint64) bool {
	if params.GlobalABrokers && len(list) > 1 {
		return true
	}
	for _, idx := range list {
		if i == idx {
			return true
		}
	}
	return false
}
func InMapKeys(i uint64, maps map[uint64]bool) bool {
	if params.GlobalABrokers && len(maps) > 1 {
		return true
	}
	for s, _ := range maps {
		if i == s {
			return true
		}
	}
	return false
}

func GetIntersection(nums1, nums2 []uint64) []uint64 {
	if params.GlobalABrokers && len(nums1) > 1 && len(nums2) > 1 {
		return nums1
	}
	m := make(map[uint64]uint64, 0)
	result := make([]uint64, 0)
	for _, v := range nums1 {
		m[v] += 1
	}
	for _, v := range nums2 {
		if m[v] > 0 {
			result = append(result, v)
		}
	}
	return result
}

func FindIntersectionShard(shard1, shard2 []uint64) uint64 {
	sl := GetIntersection(shard1, shard2)
	if len(sl) == 0 {
		log.Panic("No Intersection Shard")
		return 9999
	}
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(len(sl))
	return sl[r]
}

func GetMapShard(shardMap map[uint64]bool) (uint64, []uint64) {
	mainShard := uint64(0)
	shardList := make([]uint64, 0)
	cnt := 0
	for s, isMain := range shardMap {
		shardList = append(shardList, s)
		if isMain {
			mainShard = s
			cnt++
		}
	}
	if cnt > 1 {
		log.Panic("myFunc：more than one mainShard")
	}
	if len(shardMap) != 0 && cnt == 0 {
		log.Panic("myFunc：no mainShard")
	}
	return mainShard, shardList
}

func EqualByteSlices(s1, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}
