package pg

import (
	"cex-hertz/biz/model"
	"cex-hertz/conf"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var PostgresClient *pgxpool.Pool
var GormDB *gorm.DB

func Init() {
	fmt.Printf("conf: %+v\n", conf.GetConf())
	// 初始化 Postgres 连接池
	pgConf := conf.GetConf().Postgres
	pool, err := pgxpool.New(context.Background(), pgConf.DSN)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to postgres: %v", err))
	}
	if err := pool.Ping(context.Background()); err != nil {
		panic(fmt.Sprintf("failed to ping postgres: %v", err))
	}
	PostgresClient = pool
}

func InitGorm() error {
	pgConf := conf.GetConf().Postgres
	dsn := pgConf.DSN
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	GormDB = db
	return nil
}

func AutoMigrate() error {
	if GormDB == nil {
		return gorm.ErrInvalidDB
	}
	return GormDB.AutoMigrate(&model.Order{}, &model.Trade{})
}

// InsertOrder 插入订单（ORM方式）
func InsertOrder(orderID, userID, pair, side, price, quantity, status string, createdAt, updatedAt int64) error {
	order := &model.Order{
		OrderID:   orderID,
		UserID:    userID,
		Pair:      pair,
		Side:      side,
		Price:     price,
		Quantity:  quantity,
		Status:    status,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return GormDB.Create(order).Error
}

// GetOrderByID 查询单个订单（ORM方式）
func GetOrderByID(orderID string) (*model.Order, error) {
	var order model.Order
	err := GormDB.Where("order_id = ?", orderID).First(&order).Error
	return &order, err
}

func InitTradeTable() error {
	if PostgresClient == nil {
		return fmt.Errorf("PostgresClient not initialized")
	}
	const createTradeTableSQL = `
	CREATE TABLE IF NOT EXISTS trades (
		trade_id VARCHAR(64) PRIMARY KEY,
		pair VARCHAR(32) NOT NULL,
		price VARCHAR(32) NOT NULL,
		quantity VARCHAR(32) NOT NULL,
		timestamp BIGINT NOT NULL,
		taker_order_id VARCHAR(64),
		maker_order_id VARCHAR(64),
		side VARCHAR(8),
		engine_id VARCHAR(32),
		taker_user VARCHAR(64),
		maker_user VARCHAR(64)
	);
	`
	_, err := PostgresClient.Exec(context.Background(), createTradeTableSQL)
	return err
}

func InsertTrade(trade map[string]interface{}) error {
	if PostgresClient == nil {
		return fmt.Errorf("PostgresClient not initialized")
	}
	_, err := PostgresClient.Exec(context.Background(),
		`INSERT INTO trades (trade_id, pair, price, quantity, timestamp, taker_order_id, maker_order_id, side, engine_id, taker_user, maker_user) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		trade["trade_id"], trade["pair"], trade["price"], trade["quantity"], trade["timestamp"], trade["taker_order_id"], trade["maker_order_id"], trade["side"], trade["engine_id"], trade["taker_user"], trade["maker_user"],
	)
	return err
}

// CreateOrder 订单 CRUD
func CreateOrder(order *model.Order) error {
	return GormDB.Create(order).Error
}

func GetOrder(orderID string) (*model.Order, error) {
	var order model.Order
	err := GormDB.Where("order_id = ?", orderID).First(&order).Error
	return &order, err
}

func ListOrders(userID, status string) ([]model.Order, error) {
	var orders []model.Order
	query := GormDB.Where("user_id = ?", userID)
	if status != "" {
		query = query.Where("status = ?", status)
	}
	err := query.Find(&orders).Error
	return orders, err
}

func UpdateOrderStatus(orderID, status string) error {
	return GormDB.Model(&model.Order{}).Where("order_id = ?", orderID).Update("status", status).Error
}

// 成交 CRUD
func CreateTrade(trade *model.Trade) error {
	return GormDB.Create(trade).Error
}

func ListTrades(pair string, limit int) ([]model.Trade, error) {
	var trades []model.Trade
	err := GormDB.Where("pair = ?", pair).Order("timestamp desc").Limit(limit).Find(&trades).Error
	return trades, err
}

func GetUserBalance(userID string) ([]model.Balance, error) {
	var balances []model.Balance
	err := GormDB.Where("user_id = ?", userID).Find(&balances).Error
	return balances, err
}

func GetUserPositions(userID string) ([]model.Position, error) {
	var positions []model.Position
	err := GormDB.Where("user_id = ?", userID).Find(&positions).Error
	return positions, err
}
