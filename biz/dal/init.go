package dal

import (
	"cex-hertz/biz/dal/kafka"
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/dal/redis"
	"cex-hertz/biz/dal/rocksdb"
)

func Init() {
	pg.Init()
	redis.Init()
	kafka.Init()
	rocksdb.Init("")
}
