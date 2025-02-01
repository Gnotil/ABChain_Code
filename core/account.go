// AccountState, AccountState2
// Some basic operation about accountState

package core

import (
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
)

// AccoutState record the details of an account, it will be saved in status trie
type AccountState struct {
	Address      utils.Address
	IsSubAccount bool
	Nonce        uint64
	Balance      *big.Int
	PublicKey    []byte // this part is not useful, abort
	StorageRoot  []byte // only for smart contract account
	CodeHash     []byte // only for smart contract account
}

func (acc *AccountState) PrintAccountState() {
	fmt.Println("Address：", acc.Address)
	fmt.Println("IsSubAccount：", acc.IsSubAccount)
	fmt.Println("Nonce：", acc.Nonce)
	fmt.Println("Balance：", acc.Balance)
	fmt.Println("PublicKey：", acc.PublicKey)
	fmt.Println("StorageRoot：", acc.StorageRoot)
	fmt.Println("CodeHash：", acc.CodeHash)
	fmt.Println("=> Encode：", acc.Encode())
}

func ConstructAccount(addr string, IsSubAccount bool) *AccountState {
	ib := big.NewInt(int64(0))
	if !IsSubAccount {
		ib.Add(ib, params.Init_Balance) // 创建原始账户
	}
	accnState := &AccountState{
		Address:      addr,
		IsSubAccount: IsSubAccount,
		Nonce:        uint64(0),
		Balance:      ib,
	}
	return accnState
}

func CopyAccountState(acc *AccountState) *AccountState {
	newState := &AccountState{
		Address:      acc.Address,
		IsSubAccount: acc.IsSubAccount,
		Nonce:        acc.Nonce,
		Balance:      acc.Balance,
		PublicKey:    acc.PublicKey,
		StorageRoot:  acc.StorageRoot,
		CodeHash:     acc.CodeHash,
	}
	return newState
}

func (acc *AccountState) Deduct(val *big.Int) bool {
	if acc.Balance.Cmp(val) < 0 {
		return false
	}
	acc.Balance.Sub(acc.Balance, val)
	return true
}

// Increase the balance of an account
func (acc *AccountState) Deposit(value *big.Int) {
	acc.Balance.Add(acc.Balance, value)
}

// Encode AccountState2 in order to store in the MPT
func (acc *AccountState) Encode() []byte {
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	//fmt.Println("1: ", encoder)
	err := encoder.Encode(acc)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// Decode AccountState2
func DecodeAccount(b []byte) *AccountState {
	var acc AccountState

	decoder := gob.NewDecoder(bytes.NewReader(b))
	err := decoder.Decode(&acc)
	if err != nil {
		log.Panic(err)
	}
	return &acc
}

// Hash AccountState2 for computing the MPT Root
func (acc *AccountState) Hash() []byte {
	h := sha256.Sum256(acc.Encode())
	return h[:]
}

func (acc *AccountState) SplitAccount(targetShard map[uint64]bool) map[uint64]*AccountState {
	shardNum := big.NewInt(int64(len(targetShard)))
	value := big.NewInt(int64(0))
	rem := big.NewInt(int64(0))
	value.QuoRem(acc.Balance, shardNum, rem)
	acc.Balance.Add(value, rem)
	acc.IsSubAccount = true
	splitAccountMap := make(map[uint64]*AccountState, 0)
	mainNum := 0
	for s, isMain := range targetShard {
		if isMain {
			if mainNum == 1 {
				log.Panic("account.SplitAccount: 有多个mainShard")
			}
			splitAccountMap[s] = acc
			mainNum++
		}
		account := ConstructAccount(acc.Address, true)
		account.Balance.Add(account.Balance, value)
		splitAccountMap[s] = account
	}
	return splitAccountMap
}

func (acc *AccountState) AggregateAccount(account *AccountState) {
	acc.Balance.Add(acc.Balance, account.Balance)
	account.Balance.Sub(account.Balance, account.Balance)
}

func (acc *AccountState) AggregateAccounts(targetShard map[uint64]bool, accList map[uint64]*AccountState) {
	for s, account := range accList {
		if account.Address != acc.Address {
			log.Panic("不能Aggregate其他Account") //是否只有主状态才能调用？
		}
		if targetShard[s] { //当前account是主账户
			continue // 更改主账户状态，得改targetShard
		}
		account.Balance.Sub(account.Balance, account.Balance)
		acc.Balance.Add(acc.Balance, account.Balance)
		delete(targetShard, s) //把当前分片删掉
	}
}

func (acc *AccountState) AdjustAccounts(oldShard, newShard map[uint64]bool, oldAccList map[uint64]*AccountState) map[uint64]*AccountState {
	reStart := false
	if len(oldShard) != len(newShard) {
		reStart = true
	}
	for olds, _ := range oldShard {
		existErr := true
		for news, _ := range newShard {
			if olds == news {
				existErr = false
				break
			}
		}
		if existErr {
			reStart = true
			break
		}
	}
	if reStart {
		acc.AggregateAccounts(oldShard, oldAccList)
		return acc.SplitAccount(newShard)
	} else {
		return oldAccList
	}
}

// =========================================================================================

type AccountState2 struct {
	AcAddress   utils.Address // this part is not useful, abort
	IsSubAcc    bool
	Nonce       uint64
	Balance     *big.Int
	StorageRoot []byte // only for smart contract account
	CodeHash    []byte // only for smart contract account
}

// Reduce the balance of an account
func (as *AccountState2) Deduct(val *big.Int) bool {
	if as.Balance.Cmp(val) < 0 {
		return false
	}
	as.Balance.Sub(as.Balance, val)
	return true
}

// Increase the balance of an account
func (s *AccountState2) Deposit(value *big.Int) {
	s.Balance.Add(s.Balance, value)
}

// Encode AccountState2 in order to store in the MPT
func (as *AccountState2) Encode() []byte {
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	err := encoder.Encode(as)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// Decode AccountState2
func DecodeAS(b []byte) *AccountState {
	var as AccountState
	decoder := gob.NewDecoder(bytes.NewReader(b))
	err := decoder.Decode(&as)
	if err != nil {
		log.Panic(err)
	}
	return &as
}

// Hash AccountState2 for computing the MPT Root
func (as *AccountState2) Hash() []byte {
	h := sha256.Sum256(as.Encode())
	return h[:]
}
