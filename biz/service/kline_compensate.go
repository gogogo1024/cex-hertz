package service

import (
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/model"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
	"sort"
	"strconv"
	"time"
)

// StartKlineCompensateTask KlineCompensateTask 定时补偿/修正 K 线任务
func StartKlineCompensateTask(consulClient *api.Client) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			<-ticker.C
			lock, err := acquireConsulLock(consulClient, "kline_compensate_lock")
			if err != nil {
				hlog.Warnf("K线补偿任务获取Consul锁失败: %v", err)
				continue
			}
			if lock == nil {
				continue
			}
			// 执行补偿逻辑
			if err := CompensateKline(); err != nil {
				hlog.Errorf("K线补偿任务执行失败: %v", err)
			}
			_ = lock.Unlock()
		}
	}()
}

// acquireConsulLock 获取分布式锁
func acquireConsulLock(client *api.Client, key string) (*api.Lock, error) {
	lock, err := client.LockOpts(&api.LockOptions{
		Key:          key,
		LockTryOnce:  true,
		LockWaitTime: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	stopCh := make(chan struct{})
	leaderCh, err := lock.Lock(stopCh)
	if err != nil || leaderCh == nil {
		return nil, nil // 未获取到锁
	}
	return lock, nil
}

// CompensateKline 补偿/修正K线逻辑（示例：补全最近1分钟K线）
func CompensateKline() error {
	end := time.Now().Truncate(time.Minute)
	start := end.Add(-time.Minute)

	db := pg.GormDB
	if db == nil {
		return nil
	}
	tradeService := NewTradeService()

	// 查询该时间段内所有有成交的交易对
	pairs, err := tradeService.GetActivePairs(start, end)
	if err != nil {
		hlog.Warnf("获取活跃交易对失败: %v", err)
		return err
	}
	for _, pair := range pairs {
		trades, err := tradeService.GetTradesByPairAndTime(pair, start, end)
		if err != nil || len(trades) == 0 {
			continue
		}
		// 按时间排序
		sort.Slice(trades, func(i, j int) bool { return trades[i].Timestamp < trades[j].Timestamp })
		open := trades[0].Price
		close := trades[len(trades)-1].Price
		high := trades[0].Price
		low := trades[0].Price
		var volume float64
		for _, t := range trades {
			price, _ := strconv.ParseFloat(t.Price, 64)
			qty, _ := strconv.ParseFloat(t.Quantity, 64)
			if price > mustParseFloat(high) {
				high = t.Price
			}
			if price < mustParseFloat(low) {
				low = t.Price
			}
			volume += qty
		}
		kline := model.Kline{
			Pair:      pair,
			Period:    "1m",
			Timestamp: start.Unix(),
			Open:      open,
			Close:     close,
			High:      high,
			Low:       low,
			Volume:    strconv.FormatFloat(volume, 'f', -1, 64),
		}
		err = pg.UpsertKline(&kline)
		if err != nil {
			hlog.Errorf("K线 upsert 失败: %v, pair=%s, ts=%d", err, pair, start.Unix())
		}

	}
	hlog.Info("K线补偿任务执行: ", zap.Time("start", start), zap.Time("end", end))
	return nil
}

func mustParseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
