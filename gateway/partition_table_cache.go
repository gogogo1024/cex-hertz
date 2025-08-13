package gateway

import (
	"cex-hertz/biz/model"
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

type PartitionTableCache struct {
	lock  sync.RWMutex
	cache *model.PartitionTable
}

func NewPartitionTableCache() *PartitionTableCache {
	return &PartitionTableCache{}
}

// 持续监听分区表变更并刷新本地缓存
func (ptc *PartitionTableCache) WatchPartitionTable(ctx context.Context, consulAddr, key string) {
	client, err := api.NewClient(&api.Config{Address: consulAddr})
	if err != nil {
		log.Fatalf("Consul连接失败: %v", err)
	}
	kv := client.KV()
	var lastIndex uint64
	for {
		select {
		case <-ctx.Done():
			return
		default:
			pair, meta, err := kv.Get(key, &api.QueryOptions{
				WaitIndex: lastIndex,
				WaitTime:  10 * time.Minute,
			})
			if err != nil {
				log.Printf("分区表拉取失败: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			if pair == nil || meta.LastIndex == lastIndex {
				continue
			}
			lastIndex = meta.LastIndex
			var pt model.PartitionTable
			if err := json.Unmarshal(pair.Value, &pt); err != nil {
				log.Printf("分区表解析失败: %v", err)
				continue
			}
			ptc.lock.Lock()
			ptc.cache = &pt
			ptc.lock.Unlock()
			log.Printf("网关分区表已刷新,index=%d", lastIndex)
		}
	}
}

// 路由查找：根据 symbol 获取 worker 节点
func (ptc *PartitionTableCache) LookupWorkerBySymbol(symbol string) (string, bool) {
	ptc.lock.RLock()
	defer ptc.lock.RUnlock()
	if ptc.cache == nil {
		return "", false
	}
	partitionIDs, ok := ptc.cache.SymbolToPartition[symbol]
	if !ok || len(partitionIDs) == 0 {
		return "", false
	}
	partition := ptc.cache.Partitions[partitionIDs[0]]
	if partition == nil || len(partition.Workers) == 0 {
		return "", false
	}
	//TODO 简单负载均衡：取第一个
	return partition.Workers[0], true
}
