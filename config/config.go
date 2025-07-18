package config

import (
	"os"
)

type Config struct {
	PostgresConn  string
	KafkaBrokers  []string
	KafkaTopic    string
	WebSocketAddr string
}

func Load() *Config {
	cfg := &Config{
		PostgresConn:  getEnv("CEX_POSTGRES_CONN", "postgres://postgres:postgres@localhost:5432/cex?sslmode=disable"),
		KafkaBrokers:  []string{getEnv("CEX_KAFKA_BROKER", "localhost:9092")},
		KafkaTopic:    getEnv("CEX_KAFKA_TOPIC", "cex_trades"),
		WebSocketAddr: getEnv("CEX_WS_ADDR", ":8080"),
	}
	return cfg
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
