package handler

import (
	"cex-hertz/biz/dal/pg"
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"strconv"
)

// GetDepth 获取深度（订单簿快照）
func GetDepth(c context.Context, ctx *app.RequestContext) {
	pair := string(ctx.Query("pair"))
	if pair == "" {
		ctx.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "pair参数不能为空"})
		return
	}
	orders, _ := pg.ListOrders("", "active")
	bidsAgg := make(map[string]struct {
		amount float64
		count  int
	})
	asksAgg := make(map[string]struct {
		amount float64
		count  int
	})
	for _, o := range orders {
		if o.Pair == pair {
			qty, _ := strconv.ParseFloat(o.Quantity, 64)
			if o.Side == "buy" {
				agg := bidsAgg[o.Price]
				agg.amount += qty
				agg.count++
				bidsAgg[o.Price] = agg
			} else if o.Side == "sell" {
				agg := asksAgg[o.Price]
				agg.amount += qty
				agg.count++
				asksAgg[o.Price] = agg
			}
		}
	}
	bids := make([]map[string]interface{}, 0, len(bidsAgg))
	for price, agg := range bidsAgg {
		bids = append(bids, map[string]interface{}{
			"price":  price,
			"amount": strconv.FormatFloat(agg.amount, 'f', -1, 64),
			"count":  agg.count,
		})
	}
	asks := make([]map[string]interface{}, 0, len(asksAgg))
	for price, agg := range asksAgg {
		asks = append(asks, map[string]interface{}{
			"price":  price,
			"amount": strconv.FormatFloat(agg.amount, 'f', -1, 64),
			"count":  agg.count,
		})
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"pair": pair,
		"bids": bids,
		"asks": asks,
	})
}

// GetTrades 获取最新成交
func GetTrades(c context.Context, ctx *app.RequestContext) {
	pair := string(ctx.Query("pair"))
	limitStr := string(ctx.Query("limit"))
	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	trades, _ := pg.ListTrades(pair, limit)
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"pair":   pair,
		"trades": trades,
	})
}

// GetTicker 获取ticker（最新价、24h量等）
func GetTicker(c context.Context, ctx *app.RequestContext) {
	pair := string(ctx.Query("pair"))
	trades, _ := pg.ListTrades(pair, 1)
	var lastPrice string
	if len(trades) > 0 {
		lastPrice = trades[0].Price
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"pair":       pair,
		"last_price": lastPrice,
	})
}

// 获取K线数据（简单示例，实际应从聚合表或缓存获取）
func GetKline(c context.Context, ctx *app.RequestContext) {
	pair := string(ctx.Query("pair"))
	period := string(ctx.Query("period"))
	limitStr := string(ctx.Query("limit"))
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	if pair == "" || period == "" {
		ctx.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "pair和period参数不能为空"})
		return
	}
	// 简单实���：按分钟聚合最近成交
	trades, _ := pg.ListTrades(pair, 1000)

	type Kline struct {
		Open      string `json:"open"`
		Close     string `json:"close"`
		High      string `json:"high"`
		Low       string `json:"low"`
		Volume    string `json:"volume"`
		Timestamp int64  `json:"timestamp"`
	}
	var klines []Kline
	if len(trades) == 0 {
		ctx.JSON(consts.StatusOK, map[string]interface{}{"pair": pair, "kline": klines})
		return
	}
	// 按 period 聚合（这里只做1m演示）
	bucket := int64(60 * 1000) // 1分钟
	if period == "1m" {
		bucket = 60 * 1000
	} else if period == "5m" {
		bucket = 5 * 60 * 1000
	} else if period == "15m" {
		bucket = 15 * 60 * 1000
	} else if period == "1h" {
		bucket = 60 * 60 * 1000
	} else if period == "1d" {
		bucket = 24 * 60 * 60 * 1000
	}
	var curKline *Kline
	var curBucket int64
	for i := len(trades) - 1; i >= 0; i-- {
		t := trades[i]
		b := t.Timestamp / bucket * bucket
		if curKline == nil || b != curBucket {
			if curKline != nil {
				klines = append(klines, *curKline)
			}
			curKline = &Kline{
				Open:      t.Price,
				Close:     t.Price,
				High:      t.Price,
				Low:       t.Price,
				Volume:    t.Quantity,
				Timestamp: b,
			}
			curBucket = b
		} else {
			curKline.Close = t.Price
			if t.Price > curKline.High {
				curKline.High = t.Price
			}
			if t.Price < curKline.Low {
				curKline.Low = t.Price
			}
			// 累加成交量
			// 这里只做字符串拼接，实际应转float累加
			curKline.Volume = curKline.Volume + "," + t.Quantity
		}
	}
	if curKline != nil {
		klines = append(klines, *curKline)
	}
	if len(klines) > limit {
		klines = klines[len(klines)-limit:]
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"pair":   pair,
		"period": period,
		"kline":  klines,
	})
}
