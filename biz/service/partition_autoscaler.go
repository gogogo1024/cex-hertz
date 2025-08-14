package service

import (
	"context"
	"sync"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// PartitionMetricsProvider 分区负载采集接口
// 可实现 Prometheus、Consul KV、Redis、本地统计等多种方式

type PartitionMetricsProvider interface {
	GetPartitionQPS(partitionID string) int
	GetSymbolQPS(symbol string) int
}

// WorkerLoadProvider worker负载采集接口，支持多种衡量方式
// 可实现分区数、QPS、CPU等多种方式

type WorkerLoadProvider interface {
	GetAllWorkers() []string
	GetWorkerLoad(worker string) int
}

// PartitionAutoScaler 自动扩缩容调度器
// 需定期采集分区负载，自动调整分区表

type PartitionAutoScaler struct {
	pm       *PartitionManager
	metrics  PartitionMetricsProvider
	interval time.Duration
	stopCh   chan struct{}
	lock     sync.Mutex
}

func NewPartitionAutoScaler(pm *PartitionManager, metrics PartitionMetricsProvider, interval time.Duration) *PartitionAutoScaler {
	return &PartitionAutoScaler{
		pm:       pm,
		metrics:  metrics,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (a *PartitionAutoScaler) Run(ctx context.Context) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.scaleIfNeeded()
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		}
	}
}

func (a *PartitionAutoScaler) Stop() {
	close(a.stopCh)
}

// selectIdleWorker 选择负载最低的worker
func (a *PartitionAutoScaler) selectIdleWorker(wlp WorkerLoadProvider) string {
	workers := wlp.GetAllWorkers()
	if len(workers) == 0 {
		return ""
	}
	minLoad := wlp.GetWorkerLoad(workers[0])
	minWorker := workers[0]
	for _, w := range workers[1:] {
		load := wlp.GetWorkerLoad(w)
		if load < minLoad {
			minLoad = load
			minWorker = w
		}
	}
	return minWorker
}

// scaleIfNeeded 采集分区负载并自动扩缩容（完善扩缩容策略）
func (a *PartitionAutoScaler) scaleIfNeeded() {
	a.lock.Lock()
	defer a.lock.Unlock()
	pt := a.pm.GetPartitionTable()
	if pt == nil {
		return
	}
	_, symbolLoad := a.collectLoads(pt)
	newPT := pt.DeepCopy()
	if newPT.Partitions == nil {
		return
	}
	wlp := &MockWorkerLoadProvider{workers: []string{"worker1", "worker2"}, workerPartitions: map[string]int{"worker1": 2, "worker2": 1}} // TODO: 替换为真实实现

	needUpdateHot := a.splitHotSymbols(newPT, symbolLoad, wlp)
	needUpdateCold := a.mergeColdSymbols(newPT, symbolLoad, wlp)

	if needUpdateHot || needUpdateCold {
		a.pm.UpdatePartitionTable(newPT)
	}
}

func (a *PartitionAutoScaler) collectLoads(pt *model.PartitionTable) (map[string]int, map[string]int) {
	partitionLoad := make(map[string]int)
	symbolLoad := make(map[string]int)
	for pid, p := range pt.Partitions {
		partitionLoad[pid] = a.metrics.GetPartitionQPS(pid)
		for _, symbol := range p.Symbols {
			symbolLoad[symbol] = a.metrics.GetSymbolQPS(symbol)
		}
	}
	return partitionLoad, symbolLoad
}

func (a *PartitionAutoScaler) splitHotSymbols(newPT *model.PartitionTable, symbolLoad map[string]int, wlp WorkerLoadProvider) bool {
	needUpdate := false
	for pid, p := range newPT.Partitions {
		hotSymbols := a.findHotSymbols(p.Symbols, symbolLoad)
		if len(hotSymbols) > 0 {
			a.removeSymbolsFromPartition(p, hotSymbols)
			for _, s := range hotSymbols {
				a.createHotPartition(newPT, pid, s, wlp)
			}
			needUpdate = true
		}
	}
	return needUpdate
}

func (a *PartitionAutoScaler) findHotSymbols(symbols []string, symbolLoad map[string]int) []string {
	var hotSymbols []string
	for _, symbol := range symbols {
		if symbolLoad[symbol] > 800 {
			hotSymbols = append(hotSymbols, symbol)
		}
	}
	return hotSymbols
}

func (a *PartitionAutoScaler) removeSymbolsFromPartition(p *model.Partition, symbolsToRemove []string) {
	hotSet := make(map[string]struct{})
	for _, s := range symbolsToRemove {
		hotSet[s] = struct{}{}
	}
	var remainSymbols []string
	for _, s := range p.Symbols {
		if _, ok := hotSet[s]; !ok {
			remainSymbols = append(remainSymbols, s)
		}
	}
	p.Symbols = remainSymbols
}

func (a *PartitionAutoScaler) createHotPartition(newPT *model.PartitionTable, pid, symbol string, wlp WorkerLoadProvider) {
	idleWorker := a.selectIdleWorker(wlp)
	newPID := pid + "_hot_" + symbol + "_" + time.Now().Format("150405")
	newPartition := &model.Partition{
		PartitionID: newPID,
		Symbols:     []string{symbol},
		Workers:     []string{idleWorker},
	}
	newPT.Partitions[newPID] = newPartition
	oldList := newPT.SymbolToPartition[symbol]
	var newList []string
	for _, oldPID := range oldList {
		if oldPID != pid {
			newList = append(newList, oldPID)
		}
	}
	newList = append(newList, newPID)
	newPT.SymbolToPartition[symbol] = newList
}

func (a *PartitionAutoScaler) mergeColdSymbols(newPT *model.PartitionTable, symbolLoad map[string]int, wlp WorkerLoadProvider) bool {
	var coldSymbols []string
	for symbol, qps := range symbolLoad {
		if qps < 5 {
			coldSymbols = append(coldSymbols, symbol)
		}
	}
	if len(coldSymbols) <= 1 {
		return false
	}
	idleWorker := a.selectIdleWorker(wlp)
	newPID := "cold_merge_" + time.Now().Format("150405")
	newPartition := &model.Partition{
		PartitionID: newPID,
		Symbols:     coldSymbols,
		Workers:     []string{idleWorker},
	}
	newPT.Partitions[newPID] = newPartition
	for _, s := range coldSymbols {
		newPT.SymbolToPartition[s] = []string{newPID}
	}
	coldSet := make(map[string]struct{})
	for _, s := range coldSymbols {
		coldSet[s] = struct{}{}
	}
	for _, p := range newPT.Partitions {
		if p.PartitionID == newPID {
			continue
		}
		var remain []string
		for _, s := range p.Symbols {
			if _, ok := coldSet[s]; !ok {
				remain = append(remain, s)
			}
		}
		p.Symbols = remain
	}
	return true
}

// mock 实现，后续可替换为 Prometheus/Redis/Consul 等

type MockPartitionMetrics struct{}

func (m *MockPartitionMetrics) GetPartitionQPS(partitionID string) int {
	return 100 // TODO: 替换为真实采集
}
func (m *MockPartitionMetrics) GetSymbolQPS(symbol string) int {
	return 100 // TODO: 替换为真实采集
}

// 在热点symbol拆分和冷门symbol合并时，分配到selectIdleWorker选出的空闲worker
// 以分区数为衡量方式的Mock实现

type MockWorkerLoadProvider struct {
	workerPartitions map[string]int // worker -> 分区数
	workers          []string
}

func (m *MockWorkerLoadProvider) GetAllWorkers() []string {
	return m.workers
}
func (m *MockWorkerLoadProvider) GetWorkerLoad(worker string) int {
	return m.workerPartitions[worker]
}
