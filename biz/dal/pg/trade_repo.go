package pg

import (
	"cex-hertz/biz/model"
	"time"
)

// QueryTradesBysymbolAndTime 查询某交易对在指定时间段的成交数据
func QueryTradesBysymbolAndTime(symbol string, start, end time.Time) ([]model.Trade, error) {
	var trades []model.Trade
	err := GormDB.Table("trades").Where("symbol = ? AND timestamp >= ? AND timestamp < ?", symbol, start.UnixMilli(), end.UnixMilli()).Find(&trades).Error
	return trades, err
}

// GetActiveTradesymbols 查询指定时间段内有成交的所有交易对
func GetActiveTradesymbols(start, end time.Time) ([]string, error) {
	var symbols []string
	err := GormDB.Model(&model.Trade{}).Distinct().Where("timestamp >= ? AND timestamp < ?", start.UnixMilli(), end.UnixMilli()).Pluck("symbol", &symbols).Error
	return symbols, err
}
