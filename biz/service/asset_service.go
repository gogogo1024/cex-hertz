package service

import (
	"fmt"
	"strconv"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"

	"gorm.io/gorm"
)

func GetUserBalance(userID string) ([]model.Balance, error) {
	var balances []model.Balance
	err := pg.GormDB.Where("user_id = ?", userID).Find(&balances).Error
	return balances, err
}

func GetUserPositions(userID string) ([]model.Position, error) {
	var positions []model.Position
	err := pg.GormDB.Where("user_id = ?", userID).Find(&positions).Error
	return positions, err
}

func GetUserPositionBySymbol(userID, symbol string) (*model.Position, error) {
	var position model.Position
	err := pg.GormDB.Where("user_id = ? AND symbol = ?", userID, symbol).First(&position).Error
	return &position, err
}

// 买入持仓（加权均价）
func BuyPosition(userID, symbol, buyQty, buyPrice string) error {
	var pos model.Position
	err := pg.GormDB.Where("user_id = ? AND symbol = ?", userID, symbol).First(&pos).Error
	buyQtyF, _ := strconv.ParseFloat(buyQty, 64)
	buyPriceF, _ := strconv.ParseFloat(buyPrice, 64)
	switch err {
	case gorm.ErrRecordNotFound:
		// 新持仓
		pos = model.Position{
			UserID:   userID,
			Symbol:   symbol,
			Volume:   buyQty,
			AvgPrice: buyPrice,
		}
		return pg.GormDB.Create(&pos).Error
	case nil:
		// 加权均价
		oldQty, _ := strconv.ParseFloat(pos.Volume, 64)
		oldAvg, _ := strconv.ParseFloat(pos.AvgPrice, 64)
		newQty := oldQty + buyQtyF
		newAvg := (oldQty*oldAvg + buyQtyF*buyPriceF) / newQty
		pos.Volume = strconv.FormatFloat(newQty, 'f', -1, 64)
		pos.AvgPrice = strconv.FormatFloat(newAvg, 'f', -1, 64)
		return pg.GormDB.Save(&pos).Error
	}
	return err
}

// 卖出持仓
func SellPosition(userID, symbol, sellQty string) error {
	var pos model.Position
	err := pg.GormDB.Where("user_id = ? AND symbol = ?", userID, symbol).First(&pos).Error
	sellQtyF, _ := strconv.ParseFloat(sellQty, 64)
	if err != nil {
		return err
	}
	oldQty, _ := strconv.ParseFloat(pos.Volume, 64)
	newQty := oldQty - sellQtyF
	if newQty < 0 {
		return fmt.Errorf("持仓不足")
	}
	pos.Volume = strconv.FormatFloat(newQty, 'f', -1, 64)
	// 卖出均价不变
	if newQty == 0 {
		pos.AvgPrice = "0"
	}
	return pg.GormDB.Save(&pos).Error
}
