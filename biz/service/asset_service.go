package service

import (
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/model"
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
