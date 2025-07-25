package service

import (
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/model"
)

// 业务层只做聚合和编排，所有数据操作通过pg.order_repo.go
func InsertOrder(orderID, userID, pair, side, price, quantity, status string, createdAt, updatedAt int64) error {
	return pg.InsertOrder(orderID, userID, pair, side, price, quantity, status, createdAt, updatedAt)
}

func ListOrders(userID, status string) ([]model.Order, error) {
	return pg.ListOrders(userID, status)
}

func GetOrderByID(orderID string) (*model.Order, error) {
	return pg.GetOrderByID(orderID)
}

func CreateOrder(order *model.Order) error {
	return pg.CreateOrder(order)
}

func UpdateOrderStatus(orderID, status string) error {
	return pg.UpdateOrderStatus(orderID, status)
}
