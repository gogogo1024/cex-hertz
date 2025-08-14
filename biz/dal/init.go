package dal

import (
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/kafka"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/redis"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/rocksdb"
)

func Init() {
	pg.Init()
	redis.Init()
	kafka.Init()
	rocksdb.Init("")
}
