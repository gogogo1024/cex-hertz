package pg

import (
	"cex-hertz/biz/model"
)

// InsertOrder 插入订单
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
	return GormDB.Create(order).Error
}

// GetOrderByID 查询单个订单
func GetOrderByID(orderID string) (*model.Order, error) {
	var order model.Order
	err := GormDB.Where("order_id = ?", orderID).First(&order).Error
	return &order, err
}

// ListOrders 查询订单列表
func ListOrders(userID, status string) ([]model.Order, error) {
	var orders []model.Order
	db := GormDB.Model(&model.Order{})
	if userID != "" {
		db = db.Where("user_id = ?", userID)
	}
	if status != "" {
		db = db.Where("status = ?", status)
	}
	err := db.Find(&orders).Error
	return orders, err
}

// CreateOrder 插入订单（结构方式）
func CreateOrder(order *model.Order) error {
	return GormDB.Create(order).Error
}

// UpdateOrderStatus 更新订单状态
func UpdateOrderStatus(orderID, status string) error {
	return GormDB.Model(&model.Order{}).Where("order_id = ?", orderID).Update("status", status).Error
}

// ListTrades 查询成交记录
func ListTrades(symbol string, limit int) ([]model.Trade, error) {
	var trades []model.Trade
	db := GormDB.Model(&model.Trade{})
	if symbol != "" {
		db = db.Where("symbol = ?", symbol)
	}
	err := db.Order("timestamp desc").Limit(limit).Find(&trades).Error
	return trades, err
}
