package model

// Partition 表示一个分区
// ID: 分区唯一标识
// Symbols: 当前分区负责的 symbol 列表
// Workers: 该分区的 worker 节点地址列表
// 可根据实际需要扩展更多元数据字段

type Partition struct {
	ID      string   `json:"id"`
	Symbols []string `json:"symbols"`
	Workers []string `json:"workers"`
}

// PartitionTable 维护 symbol 到分区的映射
// Key: symbol, Value: 分区ID

type PartitionTable struct {
	SymbolToPartition map[string]string     `json:"symbol_to_partition"`
	Partitions        map[string]*Partition `json:"partitions"`
}

// NewPartitionTable 创建空分区表
func NewPartitionTable() *PartitionTable {
	return &PartitionTable{
		SymbolToPartition: make(map[string]string),
		Partitions:        make(map[string]*Partition),
	}
}
