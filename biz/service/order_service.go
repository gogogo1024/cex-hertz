package service

import (
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// InsertOrder 业务层只做聚合和编排，所有数据操作通过pg.order_repo.go
func InsertOrder(orderID, userID, symbol, side, price, quantity, status string, createdAt, updatedAt int64) error {
	order := &model.Order{
		OrderID:   orderID,
		UserID:    userID,
		Symbol:    symbol,
		Side:      side,
		Price:     price,
		Quantity:  quantity,
		Status:    status,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return pg.InsertOrder(order)
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
