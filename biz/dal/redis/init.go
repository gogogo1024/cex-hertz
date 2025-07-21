package redis

import (
	"cex-hertz/conf"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

func Init() {
	// print conf
	fmt.Printf("conf: %+v\n", conf.GetConf())
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     conf.GetConf().Redis.Address,
		Username: conf.GetConf().Redis.Username,
		Password: conf.GetConf().Redis.Password,
		DB:       conf.GetConf().Redis.DB,
	})
	if err := RedisClient.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}
}
