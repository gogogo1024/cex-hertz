package service

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/redis"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"

	"gorm.io/gorm"
)

var klinePeriods = []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"}
var klinePeriodSeconds = map[string]int64{
	"1m":  60,
	"5m":  300,
	"15m": 900,
	"30m": 1800,
	"1h":  3600,
	"4h":  14400,
	"1d":  86400,
	"1w":  604800,
	"1M":  2592000, // 30天
}

// createNewKline creates a new Kline object and saves it to the database.
func createNewKline(symbol, period string, bucket int64, price, qty string) *model.Kline {
	k := &model.Kline{
		Symbol:    symbol,
		Period:    period,
		Timestamp: bucket,
		Open:      price,
		Close:     price,
		High:      price,
		Low:       price,
		Volume:    qty,
	}
	_ = pg.CreateKline(k)
	return k
}

// updateExistingKline updates an existing Kline object and saves it to the database.
func updateExistingKline(k *model.Kline, price, qty string) {
	if price > k.High {
		k.High = price
	}
	if price < k.Low {
		k.Low = price
	}
	k.Close = price
	// 累加成交量
	if v, err := strconv.ParseFloat(k.Volume, 64); err == nil {
		if q, err := strconv.ParseFloat(qty, 64); err == nil {
			k.Volume = strconv.FormatFloat(v+q, 'f', -1, 64)
		}
	}
	_ = pg.UpdateKline(k)
}

// UpdateKlines 聚合并写入多周期K线
func UpdateKlines(symbol, price, qty string, ts int64) {
	for _, period := range klinePeriods {
		bucket := ts / klinePeriodSeconds[period] * klinePeriodSeconds[period]
		k, err := pg.GetKline(symbol, period, bucket)
		if err == gorm.ErrRecordNotFound {
			k = createNewKline(symbol, period, bucket, price, qty)
		} else if err == nil {
			updateExistingKline(k, price, qty)
		}
		// 写入Redis
		b, _ := json.Marshal(k)
		redisKey := "kline:" + symbol + ":" + period
		redis.Client.RPush(context.Background(), redisKey, b)
		redis.Client.LTrim(context.Background(), redisKey, -1000, -1) // 只保留最新1000条
	}
}
