package model

type Kline struct {
	ID        uint   `gorm:"primaryKey"`
	Symbol    string `gorm:"index:idx_symbol_period_time"`
	Period    string `gorm:"index:idx_symbol_period_time"`
	Timestamp int64  `gorm:"index:idx_symbol_period_time"`
	Open      string
	Close     string
	High      string
	Low       string
	Volume    string
}

func (Kline) TableName() string {
	return "kline"
}
