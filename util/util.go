package util

import (
	"github.com/sony/sonyflake"
	"sync"
)

var (
	sonyFlake *sonyflake.Sonyflake
	once      sync.Once
)

// InitSonyFlake 初始化 Snowflake 实例
func InitSonyFlake() {
	once.Do(func() {
		sonyFlake = sonyflake.NewSonyflake(sonyflake.Settings{})
	})
}

// GenerateOrderID 生成唯一订单ID
func GenerateOrderID() (uint64, error) {
	if sonyFlake == nil {
		InitSonyFlake()
	}
	return sonyFlake.NextID()
}
