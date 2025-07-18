package main

import (
	"github.com/joho/godotenv"
	"log"

	"cex/biz/service"
	"cex/config"
	"cex/server"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	// 初始化 PostgreSQL 连接池
	if err := service.InitPostgresPool(cfg.PostgresConn); err != nil {
		log.Fatalf("Postgres init error: %v", err)
	}

	// 初始化 Kafka Writer
	service.InitKafkaWriter(cfg.KafkaBrokers, cfg.KafkaTopic)

	// 启动 WebSocket 服务
	h := server.NewWebSocketServer(cfg.WebSocketAddr)
	log.Printf("WebSocket server started at %s", cfg.WebSocketAddr)
	h.Spin()
}
