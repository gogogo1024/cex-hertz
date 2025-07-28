package pg

import (
	"cex-hertz/biz/model"
	"time"
)

// QueryTradesByPairAndTime 查询某交易对在指定时间段的成交数据
func QueryTradesByPairAndTime(pair string, start, end time.Time) ([]model.Trade, error) {
	var trades []model.Trade
	err := GormDB.Table("trades").Where("pair = ? AND timestamp >= ? AND timestamp < ?", pair, start.UnixMilli(), end.UnixMilli()).Find(&trades).Error
	return trades, err
}

// GetActiveTradePairs 查询指定时间段内有成交的所有交易对
func GetActiveTradePairs(start, end time.Time) ([]string, error) {
	var pairs []string
	err := GormDB.Model(&model.Trade{}).Distinct().Where("timestamp >= ? AND timestamp < ?", start.UnixMilli(), end.UnixMilli()).Pluck("pair", &pairs).Error
	return pairs, err
}
