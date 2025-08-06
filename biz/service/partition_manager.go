package service

import (
	"cex-hertz/biz/model"
	"context"
	"encoding/json"
	"github.com/hashicorp/consul/api"
	"sync"
	"time"
)

const partitionTableConsulKey = "partition/table"

// PartitionManager 管理分区表，支持 Consul 存储和热加载
// 提供分区表的读写、监听和本地缓存

type PartitionManager struct {
	client      *api.Client
	cache       *model.PartitionTable
	lock        sync.RWMutex
	watchCancel context.CancelFunc
}

// NewPartitionManager 创建 PartitionManager
func NewPartitionManager(consulAddr string) (*PartitionManager, error) {
	cfg := api.DefaultConfig()
	cfg.Address = consulAddr
	cli, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	pm := &PartitionManager{
		client: cli,
		cache:  model.NewPartitionTable(),
	}
	return pm, nil
}

// LoadFromConsul 加载分区表到本地缓存
func (pm *PartitionManager) LoadFromConsul() error {
	kv := pm.client.KV()
	pair, _, err := kv.Get(partitionTableConsulKey, nil)
	if err != nil {
		return err
	}
	if pair == nil {
		return nil // 未初始化
	}
	var pt model.PartitionTable
	err = json.Unmarshal(pair.Value, &pt)
	if err != nil {
		return err
	}
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.cache = &pt
	return nil
}

// SaveToConsul 保存分区表到 Consul
func (pm *PartitionManager) SaveToConsul() error {
	pm.lock.RLock()
	data, err := json.Marshal(pm.cache)
	pm.lock.RUnlock()
	if err != nil {
		return err
	}
	kv := pm.client.KV()
	_, err = kv.Put(&api.KVPair{Key: partitionTableConsulKey, Value: data}, nil)
	return err
}

// GetPartitionTable 获取本地分区表快照
func (pm *PartitionManager) GetPartitionTable() *model.PartitionTable {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	return pm.cache
}

// WatchPartitionTable 启动分区表变更监听，自动热加载
func (pm *PartitionManager) WatchPartitionTable(ctx context.Context, interval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	pm.watchCancel = cancel
	go func() {
		var lastIndex uint64
		for {
			select {
			case <-ctx.Done():
				return
			default:
				kv := pm.client.KV()
				pair, meta, err := kv.Get(partitionTableConsulKey, nil)
				if err == nil && pair != nil && meta.LastIndex != lastIndex {
					var pt model.PartitionTable
					if err := json.Unmarshal(pair.Value, &pt); err == nil {
						pm.lock.Lock()
						pm.cache = &pt
						pm.lock.Unlock()
						lastIndex = meta.LastIndex
					}
				}
				time.Sleep(interval)
			}
		}
	}()
}

// StopWatch 停止监听
func (pm *PartitionManager) StopWatch() {
	if pm.watchCancel != nil {
		pm.watchCancel()
	}
}

// UpdatePartitionTable 更新本地分区表并保存到 Consul
func (pm *PartitionManager) UpdatePartitionTable(pt *model.PartitionTable) error {
	pm.lock.Lock()
	pm.cache = pt
	pm.lock.Unlock()
	return pm.SaveToConsul()
}
