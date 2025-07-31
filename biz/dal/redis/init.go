package redis

import (
	"cex-hertz/conf"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

var Client *redis.Client

func Init() {
	// print conf
	fmt.Printf("conf: %+v\n", conf.GetConf())
	Client = redis.NewClient(&redis.Options{
		Addr:     conf.GetConf().Redis.Address,
		Username: conf.GetConf().Redis.Username,
		Password: conf.GetConf().Redis.Password,
		DB:       conf.GetConf().Redis.DB,
	})
	if err := Client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}
}
