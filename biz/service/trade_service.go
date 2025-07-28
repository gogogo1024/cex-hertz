package service

import (
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/model"
	"time"
)

// TradeService 封装，调用包级别函数
type TradeService struct{}

func NewTradeService() *TradeService {
	return &TradeService{}
}

// GetTradesByPairAndTime 查询某交易对在指定时间段的成交数据
func (s *TradeService) GetTradesByPairAndTime(pair string, start, end time.Time) ([]model.Trade, error) {
	return pg.QueryTradesByPairAndTime(pair, start, end)
}

// GetActivePairs 查询指定时间段内有成交的所有交易对
func (s *TradeService) GetActivePairs(start, end time.Time) ([]string, error) {
	return pg.GetActiveTradePairs(start, end)
}
