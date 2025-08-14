package pg

import (
	"github.com/gogogo1024/cex-hertz-backend/biz/model"

	"gorm.io/gorm/clause"
)

// UpsertKline upsert一条K线数据
func UpsertKline(kline *model.Kline) error {
	return GormDB.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "symbol"}, {Name: "period"}, {Name: "timestamp"}},
			DoUpdates: clause.AssignmentColumns([]string{"open", "close", "high", "low", "volume"}),
		},
	).Create(kline).Error
}

// GetKline 查询一条K线
func GetKline(symbol, period string, timestamp int64) (*model.Kline, error) {
	var k model.Kline
	err := GormDB.Where("symbol = ? AND period = ? AND timestamp = ?", symbol, period, timestamp).First(&k).Error
	if err != nil {
		return nil, err
	}
	return &k, nil
}

// CreateKline 新建一条K线
func CreateKline(k *model.Kline) error {
	return GormDB.Create(k).Error
}

// UpdateKline 更新一条K线
func UpdateKline(k *model.Kline) error {
	return GormDB.Save(k).Error
}
