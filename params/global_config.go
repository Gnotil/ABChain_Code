package params

var (
	// The following parameters can be set in main.go.
	// default values:
	NodesInShard   = 4         // # of Nodes in a shard.
	ShardNum       = 8         // # of shards.
	ExpDataRootDir = "expTest" // The root dir where the experimental data should locate.
)

var (
	Block_Interval      = 5000    // The time interval for generating a new block
	MaxBlockSize_global = 2000    // The maximum number of transactions a block contains
	InjectSpeed         = 3000    // The speed of transaction injection
	TotalDataSize       = 1000000 // The total number of txs to be injected
	BatchSize           = 20000   // The supervisor read a batch of txs then send them. The size of a batch is 'BatchSize'

	BrokerNum = 90 // The # of Broker accounts used in Broker / CLPA_Broker.

	DataWrite_path     = ExpDataRootDir + "/result/"   // Measurement data result output path
	LogWrite_path      = ExpDataRootDir + "/log"       // Log output path
	DatabaseWrite_path = ExpDataRootDir + "/database/" // database write path

	SupervisorAddr = "127.0.0.1:18800" // Supervisor ip address
)

var (
	FileInput0     = `../DataTX/0to999999_BlockTransaction.csv`
	FileInput1     = `../DataTX/1000000to1999999_BlockTransaction.csv`
	FileInput2     = `../DataTX/2000000to2999999_BlockTransaction.csv`
	FileInput3     = `../DataTX/3000000to3999999_BlockTransaction.csv`
	FileInput4     = `../DataTX/4000000to4999999_BlockTransaction.csv`
	FileInput5     = `../DataTX/5000000to5999999_BlockTransaction.csv`
	FileInput6     = `../DataTX/6000000to6999999_BlockTransaction.csv`
	FileInput7     = `../DataTX/7000000to7999999_BlockTransaction.csv`
	FileInput8     = `../DataTX/8000000to8999999_BlockTransaction.csv`
	FileInput9     = `../DataTX/9000000to9999999_BlockTransaction.csv`
	FileInput10    = `../DataTX/10000000to10999999_BlockTransaction.csv`
	FileInput11    = `../DataTX/11000000to11999999_BlockTransaction.csv`
	FileInput12    = `../DataTX/12000000to12999999_BlockTransaction.csv`
	FileInputSet   = []string{FileInput0, FileInput1, FileInput2, FileInput3, FileInput4, FileInput5, FileInput6, FileInput7, FileInput8, FileInput9, FileInput10, FileInput11, FileInput12}
	FileIndexBegin = 2
	FileIndexEnd   = 2
	FileInput      = FileInput2
)

var (
	ReconfigTimeGap = 50 // The time gap between epochs. This variable is only used in CLPA / CLPA_Broker now.

	MaxIterations = 25
	ScorePower    = 4.0 //default 5
	COPRA_v       = 2.0

	PTxWeight = 0

	CandidateNum = 10 // 每次partition把最热的10个账户挑出来，排序

	ABridgesInitNum = BrokerNum // 初始化时，添加10个账户到DB
	ABridgesAddNum  = BrokerNum // 排序后，取前5个，添加到DB
	IdleBridgeNum   = BrokerNum

	GlobalABridges = true // 是否将AB设置为全局Broker
	// InitBridgeMod : "Random", "First", "Preset","Algo"
	InitBridgeMod = "CodeAlgo" // Algo, Rand, PreSet, CodeAlgo, CodeRand, CodePreSet
)

var (
	Mod           = 0 //"AdaptiveBridges", "CLPA_Broker", "CLPA", "Broker", "Relay"
	Committee     = "AdaptiveBridges"
	MeasureMethod = MeasureABMod

	PredSize = 5000
)
