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

// PartitionTable 维护 symbol 到分区的多对多映射
// Key: symbol, Value: 分区ID列表

type PartitionTable struct {
	SymbolToPartition map[string][]string   `json:"symbol_to_partition"`
	Partitions        map[string]*Partition `json:"partitions"`
}

// NewPartitionTable 创建空分区表
func NewPartitionTable() *PartitionTable {
	return &PartitionTable{
		SymbolToPartition: make(map[string][]string),
		Partitions:        make(map[string]*Partition),
	}
}

// DeepCopy Partition 的深拷贝方法
func (p *Partition) DeepCopy() *Partition {
	if p == nil {
		return nil
	}
	newSymbols := make([]string, len(p.Symbols))
	copy(newSymbols, p.Symbols)
	newWorkers := make([]string, len(p.Workers))
	copy(newWorkers, p.Workers)
	return &Partition{
		ID:      p.ID,
		Symbols: newSymbols,
		Workers: newWorkers,
	}
}

// DeepCopy PartitionTable 的深拷贝方法
func (pt *PartitionTable) DeepCopy() *PartitionTable {
	if pt == nil {
		return nil
	}
	newSymbolToPartition := make(map[string][]string, len(pt.SymbolToPartition))
	for k, v := range pt.SymbolToPartition {
		newList := make([]string, len(v))
		copy(newList, v)
		newSymbolToPartition[k] = newList
	}
	newPartitions := make(map[string]*Partition, len(pt.Partitions))
	for k, v := range pt.Partitions {
		newPartitions[k] = v.DeepCopy()
	}
	return &PartitionTable{
		SymbolToPartition: newSymbolToPartition,
		Partitions:        newPartitions,
	}
}
