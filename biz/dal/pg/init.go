package pg

import (
	"cex-hertz/conf"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

var PostgresClient *pgxpool.Pool

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
