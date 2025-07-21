package handler

import (
	"cex-hertz/biz/service"
	"cex-hertz/server"
	"encoding/json"
	"github.com/cloudwego/netpoll"
	"github.com/hertz-contrib/websocket"
	"log"
)

// ConnContext 连接上下文，后续可扩展
// 可挂载到 netpoll.Connection ��� Context
// 包含订阅频道等信息

type ConnContext struct {
	Conn     netpoll.Connection
	Channels map[string]struct{} // 已订阅频道
}

// SubmitOrderMsg 提交订单消息格式
type SubmitOrderMsg struct {
	OrderID  string `json:"order_id"`
	Pair     string `json:"pair"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// OnMessage 处理收到的 WebSocket 消息
func OnMessage(ctx *ConnContext, data []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("invalid message: %v", err)
		return
	}

	action, _ := msg["action"].(string)
	channel, _ := msg["channel"].(string)

	switch action {
	case "subscribe":
		if channel != "" {
			ctx.Channels[channel] = struct{}{}
			ack := map[string]interface{}{
				"type":    "subscription_ack",
				"channel": channel,
			}
			ackBytes, _ := json.Marshal(ack)
			ctx.Conn.Write(ackBytes)
		}
	case "unsubscribe":
		if channel != "" {
			delete(ctx.Channels, channel)
		}
	case "SubmitOrder":
		var order SubmitOrderMsg
		if err := json.Unmarshal(data, &order); err != nil {
			log.Printf("invalid SubmitOrder: %v", err)
			return
		}
		if order.Pair == "" {
			resp := map[string]interface{}{
				"type": "error",
				"msg":  "pair required",
			}
			respBytes, _ := json.Marshal(resp)
			ctx.Conn.Write(respBytes)
			return
		}
		// 分布式路由逻辑已由中间件处理，这里只需本地撮合
		service.Engine.SubmitOrder(order)
		resp := map[string]interface{}{
			"type":     "order_ack",
			"order_id": order.OrderID,
			"pair":     order.Pair,
			"status":   "received",
		}
		respBytes, _ := json.Marshal(resp)
		ctx.Conn.Write(respBytes)

		// 撮合结果推送（示例，实际应由撮合引擎回调，这里演示直接广播）
		// 假设撮合结果如下：
		matchResult := map[string]interface{}{
			"channel": order.Pair,
			"type":    "match_result",
			"data": map[string]interface{}{
				"order_id": order.OrderID,
				"status":   "matched",
			},
		}
		matchBytes, _ := json.Marshal(matchResult)
		// 广播到频道
		importServerBroadcast(order.Pair, matchBytes)
	default:
		log.Printf("unknown action: %s", action)
	}
}

// OnClose 连接关闭时清理资源
func OnClose(ctx *ConnContext) {
	for channel := range ctx.Channels {
		importServerUnsubscribe(channel, ctx.Conn)
	}
	ctx.Channels = nil
}

// --- 与 server/websocket_server.go 对接 ---
// importServerBroadcast 调用 server 层频道广播
func importServerBroadcast(channel string, msg []byte) {
	server.Broadcast(channel, msg)
}

// importServerUnsubscribe 退订频道
func importServerUnsubscribe(channel string, conn netpoll.Connection) {
	// 需要将 netpoll.Connection 转换为 *websocket.Conn
	if wsConn, ok := getWebSocketConn(conn); ok {
		shard := server.GetShard(channel)
		shard.Mu.Lock()
		if conns, ok := shard.Subs[channel]; ok {
			delete(conns, wsConn)
			if len(conns) == 0 {
				delete(shard.Subs, channel)
			}
		}
		shard.Mu.Unlock()
	}
}

// getWebSocketConn 示例：实际需根据你的 netpoll/websocket 封装实现
func getWebSocketConn(conn netpoll.Connection) (*websocket.Conn, bool) {
	ws, ok := conn.(interface{ UnderlyingConn() *websocket.Conn })
	if !ok {
		return nil, false
	}
	return ws.UnderlyingConn(), true
}
