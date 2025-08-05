package engine

import (
	"bytes"
	"cex-hertz/biz/model"
	"github.com/panjf2000/ants/v2"
	"sync"
)

var BufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

var MsgBytePool = sync.Pool{
	New: func() any {
		return make([]byte, 4096)
	},
}

var BroadcastPool *ants.Pool

func InitBroadcastPool(size int) error {
	pool, err := ants.NewPool(size)
	if err != nil {
		return err
	}
	BroadcastPool = pool
	return nil
}

type Engine interface {
	SubmitOrder(order model.SubmitOrderMsg)
	// 可扩展更多方法
}

// Broadcaster 广播回调类型
type Broadcaster func(symbol string, msg []byte)

// Unicaster 单播回调类型
type Unicaster func(userID string, msg []byte)
