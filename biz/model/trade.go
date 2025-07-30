package model

import (
	"gorm.io/gorm"
)

// Trade 成交模型（GORM）
type Trade struct {
	TradeID      string         `gorm:"primaryKey;column:trade_id" json:"trade_id"`
	Symbol       string         `gorm:"column:symbol" json:"symbol"`
	Price        string         `gorm:"column:price" json:"price"`
	Quantity     string         `gorm:"column:quantity" json:"quantity"`
	Timestamp    int64          `gorm:"column:timestamp" json:"timestamp"`
	TakerOrderID string         `gorm:"column:taker_order_id" json:"taker_order_id"`
	MakerOrderID string         `gorm:"column:maker_order_id" json:"maker_order_id"`
	Side         string         `gorm:"column:side" json:"side"`
	EngineID     string         `gorm:"column:engine_id" json:"engine_id"`
	TakerUser    string         `gorm:"column:taker_user" json:"taker_user"`
	MakerUser    string         `gorm:"column:maker_user" json:"maker_user"`
	DeletedAt    gorm.DeletedAt `gorm:"index" json:"-"`
}

func (Trade) TableName() string {
	return "trades"
}
