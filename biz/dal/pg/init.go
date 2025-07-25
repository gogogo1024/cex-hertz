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

func InitTradeTable() error {
	if GormDB == nil {
		return gorm.ErrInvalidDB
	}
	return GormDB.AutoMigrate(&model.Trade{})
}
