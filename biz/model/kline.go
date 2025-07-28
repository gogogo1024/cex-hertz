package model

type Kline struct {
	ID        uint   `gorm:"primaryKey"`
	Pair      string `gorm:"index:idx_pair_period_time"`
	Period    string `gorm:"index:idx_pair_period_time"`
	Timestamp int64  `gorm:"index:idx_pair_period_time"`
	Open      string
	Close     string
	High      string
	Low       string
	Volume    string
}

func (Kline) TableName() string {
	return "kline"
}
