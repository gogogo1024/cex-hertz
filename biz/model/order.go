package model

import (
	"gorm.io/gorm"
)

// SubmitOrderMsg 订单结构体（与 handler 保持一致）
type SubmitOrderMsg struct {
	OrderID  string
	Pair     string
	Side     string
	Price    string
	Quantity string
	UserID   string // 新增 UserID 字段
}

// Order Order 订单模型（GORM）
type Order struct {
	OrderID   string         `gorm:"primaryKey;column:order_id" json:"order_id"`
	UserID    string         `gorm:"column:user_id" json:"user_id"`
	Pair      string         `gorm:"column:pair" json:"pair"`
	Side      string         `gorm:"column:side" json:"side"`
	Price     string         `gorm:"column:price" json:"price"`
	Quantity  string         `gorm:"column:quantity" json:"quantity"`
	Status    string         `gorm:"column:status" json:"status"`
	CreatedAt int64          `gorm:"column:created_at" json:"created_at"`
	UpdatedAt int64          `gorm:"column:updated_at" json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

func (Order) TableName() string {
	return "orders"
}
