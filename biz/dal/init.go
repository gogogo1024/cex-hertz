package dal

import (
	"cex-hertz/biz/dal/kafka"
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/dal/redis"
)

func Init() {
	pg.Init()
	redis.Init()
	kafka.Init()

}
