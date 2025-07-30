package main

import (
	"cex-hertz/conf"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	address := conf.GetConf().Hertz.Address

	//// 初始化 PostgreSQL 连接池
	//if err := service.InitPostgresPool(cfg.PostgresConn); err != nil {
	//	log.Fatalf("Postgres init error: %v", err)
	//}
	//
	//// 初始化 Kafka Writer
	//service.InitKafkaWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	//
	//// 初始化 Consul 并注册撮合引擎节点
	//if err := service.InitMatchEngine(cfg.ConsulAddr, cfg.NodeID, cfg.Matchsymbols, cfg.MatchPort); err != nil {
	//	log.Fatalf("Consul注册撮合引擎失败: %v", err)
	//}

	// 启动 WebSocket 服务
	h := server.New(server.WithHostPorts(address))
	//h := server.NewWebSocketServer(cfg.WebSocketAddr)
	//log.Printf("WebSocket server started at %s", cfg.WebSocketAddr)
	h.Spin()
}
