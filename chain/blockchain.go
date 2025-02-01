// Here the blockchain structrue is defined
// each node in this system will maintain a blockchain object.

package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/storage"
	"blockEmulator/utils"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type BlockChain struct {
	db           ethdb.Database      // the leveldb database to store in the disk, for status trie
	triedb       *trie.Database      // the trie database which helps to store the status trie
	ChainConfig  *params.ChainConfig // the chain configuration, which can help to identify the chain
	CurrentBlock *core.Block         // the top block in this blockchain
	Storage      *storage.Storage    // Storage is the bolt-db to store the blocks
	Txpool       *core.TxPool        // the transaction pool
	//PartitionMap map[string]uint64   // the partition map which is defined by some algorithm can help account parition
	pmlock sync.RWMutex

	AllocationMap map[string]uint64 // 使用map去重
	ABridges      core.Bridge
	AccountState  map[string]*core.AccountState // 账户在本分片是否有状态
	Aslock        sync.RWMutex
}

// Get the transaction root, this root can be used to check the transactions
func GetTxTreeRoot(txs []*core.Transaction) []byte {
	// use a memory trie database to do this, instead of disk database
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txs {
		transactionTree.Update(tx.TxHash, tx.Encode())
	}
	return transactionTree.Hash().Bytes()
}

// Write Partition Map
//func (bc *BlockChain) Update_PartitionMap(key string, val uint64) {
//	bc.pmlock.Lock()
//	defer bc.pmlock.Unlock()
//	bc.PartitionMap[key] = val
//}

// Get parition (if not exist, return default)
//func (bc *BlockChain) Get_PartitionMap(key string) uint64 {
//	bc.pmlock.RLock()
//	defer bc.pmlock.RUnlock()
//	if _, ok := bc.PartitionMap[key]; !ok {
//		return uint64(utils.Addr2Shard(key))
//	}
//	return bc.PartitionMap[key]
//}

// Send a transaction to the pool (need to decide which pool should be sended)
func (bc *BlockChain) SendTx2Pool(txs []*core.Transaction) {
	bc.Txpool.AddTxs2Pool(txs)
}

// handle transactions and modify the status trie
func (bc *BlockChain) GetUpdateStatusTrie(txs []*core.Transaction) common.Hash {
	fmt.Printf("Blockchain.GetUpdateStatusTrie: The len of txs is %d\n", len(txs))
	// the empty block (length of txs is 0) condition
	if len(txs) == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	// build trie from the triedb (in disk)
	StateRootHash := common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	trieID := trie.TrieID(StateRootHash)
	st, err := trie.New(trieID, bc.triedb)
	if err != nil {
		fmt.Println()
		fmt.Println("StateRoot: ", bc.CurrentBlock.Header.StateRoot)
		fmt.Println("StateRootHash: ", StateRootHash)
		fmt.Println("trieID: ", trieID)
		//fmt.Println("bc.triedb: ", bc.triedb)
		fmt.Println("Blockchain.GetUpdateStatusTrie:  ", err)
		log.Panic()
	}
	//fmt.Println("1 - ", st.Hash())
	cnt := 0
	// handle transactions, the signature check is ignored here
	for i, tx := range txs {
		// fmt.Printf("tx %d: %s, %s\n", i, tx.Sender, tx.Recipient)
		// senderIn := false
		_, senderMainShard := bc.Get_AllocationMap(tx.Sender)
		_, recipientMainShard := bc.Get_AllocationMap(tx.Recipient)
		SenderIsBridge := bc.ABridges.IsBridge(tx.Sender) == 1
		RecipientIsBridge := bc.ABridges.IsBridge(tx.Recipient) == 1 // active
		if !tx.Relayed && (senderMainShard == bc.ChainConfig.ShardID || (SenderIsBridge || RecipientIsBridge)) {
			// senderIn = true
			// fmt.Printf("the sender %s is in this shard %d, \n", tx.Sender, bc.ChainConfig.ShardID)
			// modify local accountstate
			s_state_enc, _ := st.Get([]byte(tx.Sender))
			var s_state *core.AccountState
			if s_state_enc == nil {
				// fmt.Println("missing account SENDER, now adding account")
				state := core.ConstructAccount(tx.Sender, false)
				state.Nonce = uint64(i)
				s_state = state
			} else {
				s_state = core.DecodeAccount(s_state_enc)
			}
			s_balance := s_state.Balance
			if s_balance.Cmp(tx.Value) == -1 {
				fmt.Printf("the balance is less than the transfer amount\n")
				continue
			}
			s_state.Deduct(tx.Value)
			st.Update([]byte(tx.Sender), s_state.Encode())
			cnt++
		}
		// recipientIn := false

		if recipientMainShard == bc.ChainConfig.ShardID || (SenderIsBridge || RecipientIsBridge) {
			// fmt.Printf("the recipient %s is in this shard %d, \n", tx.Recipient, bc.ChainConfig.ShardID)
			// recipientIn = true
			// modify local state
			r_state_enc, _ := st.Get([]byte(tx.Recipient))
			var r_state *core.AccountState
			if r_state_enc == nil {
				// fmt.Println("missing account RECIPIENT, now adding account")
				state := core.ConstructAccount(tx.Recipient, false)
				state.Nonce = uint64(i)
				r_state = state
			} else {
				r_state = core.DecodeAccount(r_state_enc)
			}
			r_state.Deposit(tx.Value)
			st.Update([]byte(tx.Recipient), r_state.Encode())
			cnt++
		}

		// if senderIn && !recipientIn {
		// 	// change this part to the pbft stage
		// 	fmt.Printf("this transaciton is cross-shard txs, will be sent to relaypool later\n")
		// }
	}
	// commit the memory trie to the database in the disk
	if cnt == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	rt, ns := st.Commit(false)
	if ns != nil { //fmt.Println("检查rrt：", rrt.Bytes())
		err = bc.triedb.Update(trie.NewWithNodeSet(ns))
		if err != nil {
			log.Panic(err)
		}
		err = bc.triedb.Commit(rt, false)
		if err != nil {
			log.Panic(err)
		}
	}
	fmt.Println("Blockchain.GetUpdateStatusTrie: modified account number is ", cnt)
	return rt
}

// generate (mine) a block, this function return a block
func (bc *BlockChain) GenerateBlock() *core.Block {
	// pack the transactions from the txpool
	txs := bc.Txpool.PackTxs(bc.ChainConfig.BlockSize)

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Now(),
	}
	// handle transactions to build root
	rt := bc.GetUpdateStatusTrie(txs)

	bh.StateRoot = rt.Bytes()
	bh.TxRoot = GetTxTreeRoot(txs)
	b := core.NewBlock(bh, txs)
	b.Header.Miner = 0
	b.Hash = b.Header.Hash()
	return b
}

// new a genisis block, this func will be invoked only once for a blockchain object
func (bc *BlockChain) NewGenisisBlock() *core.Block {
	fmt.Println("BlockChain.NewGenisisBlock: New GenisisBlock")
	body := make([]*core.Transaction, 0)
	bh := &core.BlockHeader{
		Number: 0,
	}
	// build a new trie database by db
	triedb := trie.NewDatabaseWithConfig(bc.db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.triedb = triedb // 初始化triedb
	statusTrie := trie.NewEmpty(triedb)
	bh.StateRoot = statusTrie.Hash().Bytes() // 初始化root
	bh.TxRoot = GetTxTreeRoot(body)          // 初始化txRoot
	b := core.NewBlock(bh, body)
	b.Hash = b.Header.Hash()
	return b
}

// add the genisis block in a blockchain
func (bc *BlockChain) AddGenisisBlock(gb *core.Block) {
	bc.Storage.AddBlock(gb)
	newestHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic()
	}
	curb, err := bc.Storage.GetBlock(newestHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = curb
}

// add a block
func (bc *BlockChain) AddBlock(b *core.Block) {
	if b.Header.Number != bc.CurrentBlock.Header.Number+1 {
		fmt.Println("BlockChain.AddBlock: the block height is not correct, ", b.Header.Number, "!=", bc.CurrentBlock.Header.Number+1)
		return
	}
	// if this block is mined by the node, the transactions is no need to be handled again
	if b.Header.Miner != bc.ChainConfig.NodeID { // is Not mined by the node
		rt := bc.GetUpdateStatusTrie(b.Body)
		fmt.Println(bc.CurrentBlock.Header.Number+1, "BlockChain.AddBlock: the root = ", rt.Bytes())
	}
	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

// new a blockchain.
// the ChainConfig is pre-defined to identify the blockchain; the db is the status trie database in disk
func NewBlockChain(cc *params.ChainConfig, db ethdb.Database) (*BlockChain, error) {
	fmt.Println("BlockChain.NewBlockChain: Generating a new blockchain", db)
	ABridges := new(core.Bridge)
	ABridges.NewBridge()
	if params.InitBridgeMod[0:4] == "Code" {
		NewActiveBridge := core.InitBrokerAddr(params.ABridgesInitNum, params.InitBridgeMod)
		ABridges.UpdateBridgeMap(NewActiveBridge)
	}
	bc := &BlockChain{
		db:            db,
		ChainConfig:   cc,
		Txpool:        core.NewTxPool(),
		Storage:       storage.NewStorage(cc),
		AllocationMap: make(map[string]uint64),
		ABridges:      *ABridges,
		AccountState:  make(map[string]*core.AccountState),
	}
	curHash, err := bc.Storage.GetNewestBlockHash()

	accountState := &core.AccountState{
		Address:      "addr",
		IsSubAccount: false,
		Nonce:        0,
	}
	accountState.Encode() // 事先调用一下

	if err != nil {
		fmt.Println("BlockChain.NewBlockChain: Get newest block hash err")
		// if the Storage bolt database cannot find the newest blockhash,
		// it means the blockchain should be built in height = 0
		if err.Error() == "cannot find the newest block hash" {
			genisisBlock := bc.NewGenisisBlock()
			bc.AddGenisisBlock(genisisBlock)
			fmt.Println("BlockChain.NewBlockChain: New genisis block")
			bc.PrintBlockChain()
			return bc, nil
		}
		log.Panic()
	}

	// there is a blockchain in the storage
	fmt.Println("Existing blockchain found")
	curb, err := bc.Storage.GetBlock(curHash)
	if err != nil {
		log.Panic()
	}

	bc.CurrentBlock = curb
	triedb := trie.NewDatabaseWithConfig(db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.triedb = triedb
	// check the existence of the trie database
	_, err = trie.New(trie.TrieID(common.BytesToHash(curb.Header.StateRoot)), triedb)
	if err != nil {
		log.Panic()
	}
	fmt.Println("The status trie can be built")
	fmt.Println("Generated a new blockchain successfully")
	return bc, nil
}

// check a block is valid or not in this blockchain config
func (bc *BlockChain) IsValidBlock(b *core.Block) error {
	if string(b.Header.ParentBlockHash) != string(bc.CurrentBlock.Hash) {
		fmt.Println("BlockChain.IsValidBlock: ")
		fmt.Println("b.Header.ParentBlockHash: ", b.Header.ParentBlockHash)
		fmt.Println("bc.CurrentBlock.Hash: ", bc.CurrentBlock.Hash)
		fmt.Println("BlockChain.IsValidBlock: the parentblock hash is not equal to the current block hash")
		return errors.New("BlockChain.IsValidBlock: the parentblock hash is not equal to the current block hash")
	} else if string(GetTxTreeRoot(b.Body)) != string(b.Header.TxRoot) {
		fmt.Println("BlockChain.IsValidBlock: the transaction root is wrong")
		return errors.New("BlockChain.IsValidBlock: the transaction root is wrong")
	}
	return nil
}

// add accounts
func (bc *BlockChain) AddAccounts(addrState []*core.AccountState) {
	fmt.Printf("BlockChain.AddAccounts: The len of accounts is %d, now adding the accounts\n", len(addrState))
	//fmt.Println("创建新block时，ParentBlockHash为：", bc.CurrentBlock.Hash)
	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Time{},
	}
	// handle transactions to build root
	rt := common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	//fmt.Println("检查rt：", rt.Bytes())
	if len(addrState) != 0 {
		st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
		if err != nil {
			log.Panic(err)
		}
		//fmt.Println("检查st：", st.Hash())
		for i, state := range addrState {
			addr := state.Address
			_, mainShard := bc.Get_AllocationMap(addr)
			_, addrShardList := bc.ABridges.GetBridgeShardList(addr)
			if len(addrShardList) == 0 {
				addrShardList = []uint64{mainShard}
			}
			//if addrMainShard == bc.ChainConfig.ShardID
			if utils.InIntList(bc.ChainConfig.ShardID, addrShardList) { // addr被分配到当前分片
				//ib := new(big.Int)
				//ib.Add(ib, params.Init_Balance2) // ib 余额
				//new_state := &core.AccountState{
				//	Address:      "addr",
				//	IsSubAccount: true,
				//	Balance:      ib,
				//	Nonce:        0,
				//}
				//addrState[i].PrintAccountState()
				err := st.Update([]byte(addr), addrState[i].Encode())
				//fmt.Println("st更新", i, "/", len(addrState), ": ", st.Hash(),"\n")
				if err != nil {
					return
				}
			}
		}
		rrt, ns := st.Commit(false)
		if ns != nil { //fmt.Println("检查rrt：", rrt.Bytes())
			err = bc.triedb.Update(trie.NewWithNodeSet(ns))
			if err != nil {
				log.Panic(err)
			}
			err = bc.triedb.Commit(rt, false)
			if err != nil {
				log.Panic(err)
			}
			rt = rrt
		}
	}

	emptyTxs := make([]*core.Transaction, 0)
	bh.StateRoot = rt.Bytes()
	//fmt.Println("检查StateRoot：", bh.StateRoot)
	bh.TxRoot = GetTxTreeRoot(emptyTxs)
	b := core.NewBlock(bh, emptyTxs)
	b.Header.Miner = 0
	b.Hash = b.Header.Hash()

	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

// fetch accounts
func (bc *BlockChain) FetchAccounts(addrs []string, isSubAcc bool) []*core.AccountState {
	res := make([]*core.AccountState, 0)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for _, addr := range addrs { // 遍历这些地址
		asenc, _ := st.Get([]byte(addr))
		var state_a *core.AccountState
		if asenc == nil {
			state := core.ConstructAccount(addr, isSubAcc)
			state_a = state
		} else {
			state_a = core.DecodeAccount(asenc)
		}
		res = append(res, state_a)
	}
	return res
}

// close a blockChain, close the database inferfaces
func (bc *BlockChain) CloseBlockChain() {
	bc.Storage.DataBase.Close()
	bc.triedb.CommitPreimages()
}

// print the details of a blockchain
func (bc *BlockChain) PrintBlockChain2() string {
	vals := []interface{}{
		bc.CurrentBlock.Header.Number,
		bc.CurrentBlock.Hash,
		bc.CurrentBlock.Header.StateRoot,
		bc.CurrentBlock.Header.Time,
		bc.triedb,
		// len(bc.Txpool.RelayPool[1]),
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

func (bc *BlockChain) PrintBlockChain() {
	fmt.Println("-------------------------------------------------------------------------------------------------------")
	fmt.Println("(Number: ", bc.CurrentBlock.Header.Number, ")")
	fmt.Println("(Hash: ", bc.CurrentBlock.Hash)
	fmt.Println("(StateRoot: ", bc.CurrentBlock.Header.StateRoot, ")")
	fmt.Println("(ParentBlockHash: ", bc.CurrentBlock.Header.ParentBlockHash, ")")
	fmt.Println("(Time: ", bc.CurrentBlock.Header.Time, ")")
	//fmt.Println("(triedb: ", bc.triedb, ")")
	fmt.Println("-------------------------------------------------------------------------------------------------------")
}
