package handler

import (
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/dal/redis"
	"cex-hertz/biz/model"
	"context"
	"encoding/json"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"sort"
	"strconv"
)

// GetDepth 获取深度（订单簿快照）
func GetDepth(c context.Context, ctx *app.RequestContext) {
	pair := string(ctx.Query("pair"))
	if pair == "" {
		ctx.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "pair参数不能为空"})
		return
	}
	// 分别获取买卖档数
	bidLimitStr := string(ctx.Query("bid_limit"))
	askLimitStr := string(ctx.Query("ask_limit"))
	bidLimit := 20
	askLimit := 20
	if bidLimitStr != "" {
		if l, err := strconv.Atoi(bidLimitStr); err == nil && l > 0 {
			bidLimit = l
		}
	}
	if askLimitStr != "" {
		if l, err := strconv.Atoi(askLimitStr); err == nil && l > 0 {
			askLimit = l
		}
	}
	orders, _ := pg.ListOrders("", "active")
	type depthItem struct {
		Price    float64
		Amount   float64
		Count    int
		PriceStr string
	}
	bidsAgg := make(map[string]*depthItem)
	asksAgg := make(map[string]*depthItem)
	for _, o := range orders {
		if o.Pair != pair {
			continue
		}
		qty, err := strconv.ParseFloat(o.Quantity, 64)
		if err != nil {
			continue
		}
		price, err := strconv.ParseFloat(o.Price, 64)
		if err != nil {
			continue
		}
		if o.Side == "buy" {
			if bidsAgg[o.Price] == nil {
				bidsAgg[o.Price] = &depthItem{Price: price, PriceStr: o.Price}
			}
			bidsAgg[o.Price].Amount += qty
			bidsAgg[o.Price].Count++
		} else if o.Side == "sell" {
			if asksAgg[o.Price] == nil {
				asksAgg[o.Price] = &depthItem{Price: price, PriceStr: o.Price}
			}
			asksAgg[o.Price].Amount += qty
			asksAgg[o.Price].Count++
		}
	}
	bids := make([]map[string]interface{}, 0, len(bidsAgg))
	asks := make([]map[string]interface{}, 0, len(asksAgg))
	for _, agg := range bidsAgg {
		bids = append(bids, map[string]interface{}{
			"price":  agg.PriceStr,
			"amount": strconv.FormatFloat(agg.Amount, 'f', -1, 64),
			"count":  agg.Count,
		})
	}
	for _, agg := range asksAgg {
		asks = append(asks, map[string]interface{}{
			"price":  agg.PriceStr,
			"amount": strconv.FormatFloat(agg.Amount, 'f', -1, 64),
			"count":  agg.Count,
		})
	}
	sort.Slice(bids, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(bids[i]["price"].(string), 64)
		pj, _ := strconv.ParseFloat(bids[j]["price"].(string), 64)
		return pi > pj
	})
	sort.Slice(asks, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(asks[i]["price"].(string), 64)
		pj, _ := strconv.ParseFloat(asks[j]["price"].(string), 64)
		return pi < pj
	})
	if len(bids) > bidLimit {
		bids = bids[:bidLimit]
	}
	if len(asks) > askLimit {
		asks = asks[:askLimit]
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"pair":      pair,
		"bids":      bids,
		"asks":      asks,
		"bid_limit": bidLimit,
		"ask_limit": askLimit,
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

// GetKline 获取K线数据（简单示例，实际应从聚合表或缓存获取）
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
	// 1. 优先查 Redis
	redisKey := "kline:" + pair + ":" + period
	klineData, err := redis.RedisClient.LRange(c, redisKey, int64(-limit), -1).Result()
	if err == nil && len(klineData) > 0 {
		var klines []model.Kline
		for _, v := range klineData {
			var k model.Kline
			if e := json.Unmarshal([]byte(v), &k); e == nil {
				klines = append(klines, k)
			}
		}
		ctx.JSON(consts.StatusOK, map[string]interface{}{
			"pair":   pair,
			"period": period,
			"kline":  klines,
		})
		return
	}
	// 2. 查数据库聚合表
	var klines []model.Kline
	db := pg.GormDB
	db.Where("pair = ? AND period = ?", pair, period).
		Order("timestamp desc").Limit(limit).Find(&klines)
	// 逆序
	for i, j := 0, len(klines)-1; i < j; i, j = i+1, j-1 {
		klines[i], klines[j] = klines[j], klines[i]
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"pair":   pair,
		"period": period,
		"kline":  klines,
	})
}
