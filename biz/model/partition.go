package model

// Partition 表示一个分区
// ID: 分区唯一标识
// Symbols: 当前分区负责的 symbol 列表
// Workers: 该分区的 worker 节点地址列表
// 可根据实际需要扩展更多元数据字段

type Partition struct {
	PartitionID string   `json:"partition_id"`
	Symbols     []string `json:"symbols"`
	Workers     []string `json:"workers"`
}

// SymbolMigrationInfo 记录 symbol 的迁移状态和新分区ID
// 可在分区扩缩容时动态维护
// Key: symbol
// Value: {migrating: bool, newPartitionID: string}
type SymbolMigrationInfo struct {
	Symbol     string `json:"symbol"`
	From       string `json:"from"`
	To         string `json:"to"`
	Migrating  bool   `json:"migrating"`
	UpdateTime int64  `json:"update_time"`
}

// PartitionTable 维护 symbol 到分区的多对多映射和迁移状态
// Key: symbol, Value: 分区ID列表
// MigrationInfo: symbol 的迁移状态表
type PartitionTable struct {
	SymbolToPartition map[string][]string             `json:"symbol_to_partition"`
	Partitions        map[string]*Partition           `json:"partitions"`
	MigrationInfo     map[string]*SymbolMigrationInfo `json:"migration_info"`
}

// NewPartitionTable 创建空分区表
func NewPartitionTable() *PartitionTable {
	return &PartitionTable{
		SymbolToPartition: make(map[string][]string),
		Partitions:        make(map[string]*Partition),
		MigrationInfo:     make(map[string]*SymbolMigrationInfo),
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
		PartitionID: p.PartitionID,
		Symbols:     newSymbols,
		Workers:     newWorkers,
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
	newMigrationInfo := make(map[string]*SymbolMigrationInfo, len(pt.MigrationInfo))
	for k, v := range pt.MigrationInfo {
		newMigrationInfo[k] = &SymbolMigrationInfo{
			Symbol:     v.Symbol,
			From:       v.From,
			To:         v.To,
			Migrating:  v.Migrating,
			UpdateTime: v.UpdateTime,
		}
	}
	return &PartitionTable{
		SymbolToPartition: newSymbolToPartition,
		Partitions:        newPartitions,
		MigrationInfo:     newMigrationInfo,
	}
}

// GetSymbolsForWorker 返回指定 worker 负责的所有 symbol（去重）
func (pt *PartitionTable) GetSymbolsForWorker(workerAddr string) []string {
	symbolSet := make(map[string]struct{})
	for _, partition := range pt.Partitions {
		for _, worker := range partition.Workers {
			if worker == workerAddr {
				for _, symbol := range partition.Symbols {
					symbolSet[symbol] = struct{}{}
				}
			}
		}
	}
	var symbols []string
	for symbol := range symbolSet {
		symbols = append(symbols, symbol)
	}
	return symbols
}
