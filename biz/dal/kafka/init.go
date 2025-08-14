package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/gogogo1024/cex-hertz-backend/conf"
	"github.com/segmentio/kafka-go"
)

var (
	writers sync.Map // map[string]*kafka.Writer
)

// GetWriter 获取指定 topic 的 kafka.Writer，自动复用
func GetWriter(topic string) *kafka.Writer {
	val, ok := writers.Load(topic)
	if ok {
		return val.(*kafka.Writer)
	}
	kafkaConf := conf.GetConf().Kafka
	brokers := kafkaConf.Brokers
	if len(brokers) == 0 {
		panic("Kafka brokers not configured")
	}
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		Async: true,
	}
	writers.Store(topic, writer)
	return writer
}

// InitWriters 预初始化所有 topics 的 writer（自动从配置获取）
func InitWriters() {
	topicsMap := conf.GetConf().Kafka.Topics
	for _, topic := range topicsMap {
		GetWriter(topic)
	}
}

// TestKafkaConnection 测试 Kafka 连接
func TestKafkaConnection() {
	kafkaConf := conf.GetConf().Kafka
	brokers := kafkaConf.Brokers
	if len(brokers) == 0 {
		panic("Kafka brokers not configured")
	}
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		panic(fmt.Sprintf("failed to connect to kafka: %v", err))
	}
	_ = conn.Close()
}

// CloseAllWriters 可选：关闭所有 writer
func CloseAllWriters() {
	writers.Range(func(key, value interface{}) bool {
		if w, ok := value.(*kafka.Writer); ok {
			_ = w.Close()
		}
		return true
	})
}

// Init 初始化 Kafka，包含连接测试和 writer 预初始化（自动从配置获取）
func Init() {
	TestKafkaConnection()
	InitWriters()
}
