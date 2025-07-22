package kafka

import (
	"cex-hertz/conf"
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

var KafkaWriter *kafka.Writer

func Init() {
	fmt.Printf("conf: %+v\n", conf.GetConf())
	kafkaConf := conf.GetConf().Kafka
	brokers := kafkaConf.Brokers
	if len(brokers) == 0 {
		panic("Kafka brokers not configured")
	}
	KafkaWriter = &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: kafkaConf.Topic,
		Async: true,
	}
	// 测试连接
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		panic(fmt.Sprintf("failed to connect to kafka: %v", err))
	}
	_ = conn.Close()
}
