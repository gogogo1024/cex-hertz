package server

import (
	"bytes"
	"cex-hertz/conf"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/hertz-contrib/websocket"
	"github.com/panjf2000/ants/v2"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
)

const shardNum = 32

var upgrader = websocket.HertzUpgrader{
	CheckOrigin: func(ctx *app.RequestContext) bool {
		return true // 允许所有跨域 WebSocket 连接
	},
} // use default options

type ChannelShard struct {
	Mu     sync.RWMutex
	Subs   map[string]map[*websocket.Conn]struct{}
	MsgBuf map[string]chan []byte // 每个频道的消息缓冲区
}

var channelShards [shardNum]*ChannelShard

var broadcastPool *ants.Pool

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

var msgBytePool = sync.Pool{
	New: func() any {
		return make([]byte, 4096)
	},
}

func init() {
	for i := 0; i < shardNum; i++ {
		channelShards[i] = &ChannelShard{
			Subs:   make(map[string]map[*websocket.Conn]struct{}),
			MsgBuf: make(map[string]chan []byte),
		}
	}
	// 初始化 goroutine 池，最大协程数可根据实际并发调整
	pool, err := ants.NewPool(1024)
	if err != nil {
		panic(err)
	}
	broadcastPool = pool
}

// 启动频道消息分发 goroutine
func ensureChannelDispatcher(shard *ChannelShard, channel string) {
	if _, ok := shard.MsgBuf[channel]; ok {
		return
	}
	msgBuf := make(chan []byte, 4096) // 环形缓冲区，容量可调
	shard.MsgBuf[channel] = msgBuf
	go func() {
		for msg := range msgBuf {
			shard.Mu.RLock()
			conns := shard.Subs[channel]
			for conn := range conns {
				err := broadcastPool.Submit(func() {
					// bytes.Buffer+sync.Pool
					success := false
					for i := 0; i < 3; i++ {
						if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
							log.Printf("broadcast error: %v, retry %d", err, i+1)
						} else {
							success = true
							break
						}
					}
					if !success {
						log.Printf("conn write failed after retries, will remove from channel: %v", conn.RemoteAddr())
						shard := GetShard(channel)
						shard.Mu.Lock()
						delete(shard.Subs[channel], conn)
						if len(shard.Subs[channel]) == 0 {
							delete(shard.Subs, channel)
						}
						shard.Mu.Unlock()
						cleanConnFromAllChannels(conn)
						_ = conn.Close()
					}
					// 归还消息缓冲区到池（如果是从池获取的）
					// msgBufferPool.Put(msg) // 若业务有 make/copy，可归还
				})
				if err != nil {
					log.Printf("broadcastPool.Submit error: %v, conn: %v", err, conn.RemoteAddr())
					// 可选：这里可以选择重试或直接 continue
				}
			}
			shard.Mu.RUnlock()
		}
		// 关闭时自动清理
		shard.Mu.Lock()
		delete(shard.MsgBuf, channel)
		shard.Mu.Unlock()
	}()
}

func GetShard(channel string) *ChannelShard {
	h := fnv32(channel)
	return channelShards[h%shardNum]
}

func fnv32(key string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// 解析 action/channel

type Message struct {
	Action  string `json:"action"`
	Channel string `json:"channel"`
}

func parseAction(msg []byte) (string, string) {
	var m Message
	if err := json.Unmarshal(msg, &m); err != nil {
		return "", ""
	}
	return m.Action, m.Channel
}

// 清理连接所有频道订阅
func cleanConnFromAllChannels(c *websocket.Conn) {
	for i := 0; i < shardNum; i++ {
		shard := channelShards[i]
		shard.Mu.Lock()
		for ch, conns := range shard.Subs {
			if conns != nil {
				if _, ok := conns[c]; ok {
					delete(conns, c)
					if len(conns) == 0 {
						delete(shard.Subs, ch)
					}
				}
			}
		}
		shard.Mu.Unlock()
	}
}

// Broadcast 广播消息到频道
func Broadcast(channel string, msg []byte) {
	shard := GetShard(channel)
	shard.Mu.Lock()
	ensureChannelDispatcher(shard, channel)
	buf, ok := shard.MsgBuf[channel]
	shard.Mu.Unlock()
	if ok && buf != nil {
		select {
		case buf <- msg:
			// 写入成功
		default:
			log.Printf("channel %s ring buffer full, drop message", channel)
			go saveDroppedMessage(channel, msg)
		}
	}
}

func safeBroadcast(channel string, buf *bytes.Buffer) {
	msg := msgBytePool.Get().([]byte)
	if cap(msg) < buf.Len() {
		msg = make([]byte, buf.Len())
	}
	msg = msg[:buf.Len()]
	copy(msg, buf.Bytes())
	Broadcast(channel, msg)
	msgBytePool.Put(msg)
}

// 丢弃的消息异步写入 Kafka
func saveDroppedMessage(channel string, msg []byte) {
	// 直接写入 Kafka，topic 可用 "dropped_{channel}"
	go func() {
		topic := "dropped_" + channel
		w := getDroppedKafkaWriter(topic)
		if w == nil {
			log.Printf("failed to get dropped kafka writer for topic %s", topic)
			return
		}
		err := w.WriteMessages(context.Background(),
			kafka.Message{Value: msg},
		)
		if err != nil {
			log.Printf("failed to write dropped message to kafka: %v", err)
		}
	}()
}

var droppedKafkaWriters sync.Map // map[topic]*kafka.Writer

func getDroppedKafkaWriter(topic string) *kafka.Writer {
	if w, ok := droppedKafkaWriters.Load(topic); ok {
		return w.(*kafka.Writer)
	}

	brokers := conf.GetConf().Kafka.Brokers
	w := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		Async: true,
	}
	droppedKafkaWriters.Store(topic, w)
	return w
}

// PushMatchResult 复杂消息组装示例：撮合结果推送
func PushMatchResult(channel string, orderID string, price string, quantity string, ts int64) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(`{"channel":"`)
	buf.WriteString(channel)
	buf.WriteString(`","type":"match_result","data":{`)
	buf.WriteString(`"order_id":"`)
	buf.WriteString(orderID)
	buf.WriteString(`","price":"`)
	buf.WriteString(price)
	buf.WriteString(`","quantity":"`)
	buf.WriteString(quantity)
	buf.WriteString(`","ts":`)
	buf.WriteString(fmt.Sprintf("%d", ts))
	buf.WriteString("}}")
	safeBroadcast(channel, buf)
	bufferPool.Put(buf)
}

// PushOrderBookSnapshot 复杂消息组装示例：订单簿快照推送
func PushOrderBookSnapshot(channel string, bids, asks []string, ts int64) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(`{"channel":"`)
	buf.WriteString(channel)
	buf.WriteString(`","type":"depth_update","data":{`)
	buf.WriteString(`"bids":[`)
	for i, b := range bids {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("\"")
		buf.WriteString(b)
		buf.WriteString("\"")
	}
	buf.WriteString("],\"asks\":[")
	for i, a := range asks {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("\"")
		buf.WriteString(a)
		buf.WriteString("\"")
	}
	buf.WriteString("],\"ts\":")
	buf.WriteString(fmt.Sprintf("%d", ts))
	buf.WriteString("}}")
	safeBroadcast(channel, buf)
	bufferPool.Put(buf)
}

// NewWebSocketServer WebSocket 服务端
func NewWebSocketServer(addr string) *server.Hertz {
	h := server.Default(server.WithHostPorts(addr))
	h.NoHijackConnPool = true
	h.GET("/ws", func(_ context.Context, c *app.RequestContext) {
		err := upgrader.Upgrade(c, func(conn *websocket.Conn) {
			defer func() {
				cleanConnFromAllChannels(conn)
				if err := conn.Close(); err != nil {
					log.Printf("close error: %v", err)
				}
			}()

			for {
				mt, msg, err := conn.ReadMessage()
				if err != nil {
					log.Printf("read error: %v", err)
					break
				}

				action, channel := parseAction(msg)
				if action == "subscribe" && channel != "" {
					shard := GetShard(channel)
					shard.Mu.Lock()
					if shard.Subs[channel] == nil {
						shard.Subs[channel] = make(map[*websocket.Conn]struct{})
					}
					shard.Subs[channel][conn] = struct{}{}
					shard.Mu.Unlock()
					ack := []byte(`{"type":"subscription_ack","channel":"` + channel + `"}`)
					if err := conn.WriteMessage(mt, ack); err != nil {
						log.Printf("ack error: %v", err)
					}
					ensureChannelDispatcher(shard, channel) // 确保启动分发 goroutine
					continue
				}
				if action == "unsubscribe" && channel != "" {
					shard := GetShard(channel)
					shard.Mu.Lock()
					if conns, ok := shard.Subs[channel]; ok {
						delete(conns, conn)
						if len(conns) == 0 {
							delete(shard.Subs, channel)
						}
					}
					shard.Mu.Unlock()
					ack := []byte(`{"type":"unsubscription_ack","channel":"` + channel + `"}`)
					if err := conn.WriteMessage(mt, ack); err != nil {
						log.Printf("ack error: %v", err)
					}
					continue
				}
				// 其他 action 处理，例如 publish/SubmitOrder
			}
		})
		if err != nil {
			log.Printf("upgrade error: %v", err)
		}
	})

	return h
}
