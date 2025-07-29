package engine

import "cex-hertz/biz/model"

type Engine interface {
	SubmitOrder(order model.SubmitOrderMsg)
	// 可扩展更多方法
}

// Broadcaster 广播回调类型
type Broadcaster func(symbol string, msg []byte)

// Unicaster 单播回调类型
type Unicaster func(userID string, msg []byte)
