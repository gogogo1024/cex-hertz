// Engine 接口定义
package service

import (
	"bytes"
	kafkaDal "cex-hertz/biz/dal/kafka"
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/dal/redis"
	"cex-hertz/biz/engine"
	"cex-hertz/biz/model"
	"cex-hertz/conf"
	"cex-hertz/util"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/segmentio/kafka-go"
	"math/rand"
	"net/http"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// 撮合引擎接口
// 你可以根据实际需要扩展更多方法

type Engine interface {
	SubmitOrder(order model.SubmitOrderMsg)
	// ...其他方法...
}

// 撮合引擎接口
// 实际业务可扩展为分 symbol 的撮合线程池

type MatchEngine struct {
	// 每个交易对一个撮合队列
	orderQueues map[string]chan model.SubmitOrderMsg
	mu          sync.RWMutex
	broadcaster engine.Broadcaster
	unicaster   engine.Unicaster
}

var (
	consulHelper *ConsulHelper
)

// 内存订单存储结构
var (
	orderStore   = make(map[string]model.SubmitOrderMsg) // orderID -> order
	userOrderMap = make(map[string][]string)             // userID -> []orderID
	orderStatus  = make(map[string]string)               // orderID -> status
	storeMu      sync.RWMutex
)

func NewMatchEngine(broadcaster engine.Broadcaster, unicaster engine.Unicaster) *MatchEngine {
	return &MatchEngine{
		orderQueues: make(map[string]chan model.SubmitOrderMsg),
		broadcaster: broadcaster,
		unicaster:   unicaster,
	}
}

// InitMatchEngineWithHelper 支持传入 ConsulHelper 实例，便于多地址高可用
func InitMatchEngineWithHelper(helper *ConsulHelper, nodeID string, symbols []string, port int) error {
	consulHelper = helper
	if err := consulHelper.RegisterMatchEngine(nodeID, symbols, port); err != nil {
		return err
	}
	hlog.Infof("MatchEngine节点已注册到Consul, nodeID=%s, symbols=%v, port=%d", nodeID, symbols, port)
	return nil
}

// SubmitOrder 持久化到Kafka，由独立消费者批量入库
func (m *MatchEngine) SubmitOrder(order model.SubmitOrderMsg) {
	// 后端生成唯一订单ID
	id, err := util.GenerateOrderID()
	if err != nil {
		hlog.Errorf("生成订单ID失败: %v", err)
		return
	}
	order.OrderID = fmt.Sprintf("%d", id)

	hlog.Infof("SubmitOrder called, order_id=%s, symbol=%s, side=%s, price=%s, quantity=%s", order.OrderID, order.Symbol, order.Side, order.Price, order.Quantity)
	m.mu.Lock()
	queue, ok := m.orderQueues[order.Symbol]
	if !ok {
		queue = make(chan model.SubmitOrderMsg, 10000) // 每个交易对一个队列
		m.orderQueues[order.Symbol] = queue
		// 启动独立撮合 worker goroutine
		go m.matchWorker(order.Symbol, queue)
	}
	m.mu.Unlock()

	storeMu.Lock()
	orderStore[order.OrderID] = order
	userOrderMap[order.UserID] = append(userOrderMap[order.UserID], order.OrderID)
	orderStatus[order.OrderID] = "active"
	storeMu.Unlock()

	// 持久化到Kafka，由独立消费者批量入库
	saveOrderToKafka(order)

	// 缓存用户活跃订单ID
	if order.UserID != "" {
		cacheUserActiveOrder(order.UserID, order.OrderID)
	}
	queue <- order
}

// Kafka订单写入相关
var orderBatchChan chan model.SubmitOrderMsg
var orderKafkaTopic string

func InitOrderKafkaWriter(topic string) {
	orderKafkaTopic = topic
	orderBatchChan = make(chan model.SubmitOrderMsg, 10000)
	go batchOrderKafkaWriter()
}

// 优雅关闭Kafka批量写入协程
var orderKafkaWriterClose = make(chan struct{})

func ShutdownOrderKafkaWriter() {
	close(orderKafkaWriterClose)
}

func batchOrderKafkaWriter() {
	batch := make([]kafka.Message, 0, 100)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case order := <-orderBatchChan:
			msgBytes, err := json.Marshal(order)
			if err == nil {
				batch = append(batch, kafka.Message{Value: msgBytes})
			}
			if len(batch) >= 100 {
				flushOrderKafkaBatch(&batch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushOrderKafkaBatch(&batch)
			}
		case <-orderKafkaWriterClose:
			// 收到关闭信号，写完剩余数据再退出
			if len(batch) > 0 {
				flushOrderKafkaBatch(&batch)
			}
			return
		}
	}
}

func flushOrderKafkaBatch(batch *[]kafka.Message) {
	if len(*batch) == 0 {
		hlog.Infof("[OrderKafkaBatch] flushOrderKafkaBatch被调用，但batch为空")
		return
	}
	writer := kafkaDal.GetWriter(orderKafkaTopic)
	if writer == nil {
		hlog.Errorf("[OrderKafkaBatch] Kafka writer未初始化，topic=%v，无法写入Kafka", orderKafkaTopic)
		return
	}
	hlog.Infof("[OrderKafkaBatch] 开始写入Kafka，topic=%v，消息数量=%d", orderKafkaTopic, len(*batch))
	err := writer.WriteMessages(context.Background(), (*batch)...)
	if err != nil {
		hlog.Errorf("[OrderKafkaBatch] 写入Kafka失败，topic=%v，err=%v", orderKafkaTopic, err)
	} else {
		hlog.Infof("[OrderKafkaBatch] 写入Kafka成功，topic=%v，消息数量=%d", orderKafkaTopic, len(*batch))
	}
	*batch = (*batch)[:0]
}

func saveOrderToKafka(order model.SubmitOrderMsg) {
	if orderBatchChan != nil {
		orderBatchChan <- order
	}
}

// Kafka订单消费批量入库相关
var orderKafkaConsumerClose chan struct{}

func StartOrderKafkaConsumer(topic string) {
	orderKafkaConsumerClose = make(chan struct{})
	brokers := conf.GetConf().Kafka.Brokers
	consumerNum := runtime.NumCPU()
	for i := 0; i < consumerNum; i++ {
		go orderKafkaConsumerWorker(i, brokers, topic)
	}
}

// 拆分后的worker逻辑
func orderKafkaConsumerWorker(idx int, brokers []string, topic string) {
	r := initOrderKafkaReader(brokers, topic)
	batch := make([]model.SubmitOrderMsg, 0, 50000)
	var batchWait time.Duration = 1 * time.Second
	var batchMinSize = 1000
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	hlog.Infof("[OrderKafkaConsumer-%d] Kafka Reader初始化: topic=%s, groupID=%s, brokers=%v", idx, topic, "order-db-writer", brokers)
	for {
		select {
		case <-orderKafkaConsumerClose:
			if len(batch) > 0 {
				batchInsertOrders(batch)
				batch = batch[:0]
			}
			return
		case <-ticker.C:
			batchWait, batchMinSize = adjustBatchParams(r)
			msgBatch := consumeOrderMessages(r, batchWait)
			if len(msgBatch) >= batchMinSize {
				batchInsertOrders(msgBatch)
			}
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			m, err := r.ReadMessage(ctx)
			if err == nil {
				var order model.SubmitOrderMsg
				if err := json.Unmarshal(m.Value, &order); err == nil {
					batch = append(batch, order)
				}
				if len(batch) >= 50000 {
					batchInsertOrders(batch)
					batch = batch[:0]
				}
			}
			cancel()
		}
	}
}

func initOrderKafkaReader(brokers []string, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "order-db-writer",
		MinBytes: 1000,
		MaxBytes: 20e6,
	})
}

func adjustBatchParams(r *kafka.Reader) (time.Duration, int) {
	stats := r.Stats()
	lag := stats.Lag
	if lag > 20000 {
		return 100 * time.Millisecond, 100
	} else if lag > 5000 {
		return 500 * time.Millisecond, 500
	}
	return 1 * time.Second, 1000
}

func consumeOrderMessages(r *kafka.Reader, batchWait time.Duration) []model.SubmitOrderMsg {
	ctx, cancel := context.WithTimeout(context.Background(), batchWait)
	defer cancel()
	msgBatch := make([]model.SubmitOrderMsg, 0, 50000)
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		var order model.SubmitOrderMsg
		if err := json.Unmarshal(m.Value, &order); err == nil {
			msgBatch = append(msgBatch, order)
		}
		if len(msgBatch) >= 50000 {
			break
		}
	}
	return msgBatch
}

func batchInsertOrders(orders []model.SubmitOrderMsg) {
	hlog.Infof("[OrderBatch] batchInsertOrders方法被调用, pgPool是否为nil: %v, 订单数量: %d", pg.GetPool() == nil, len(orders))
	if pg.GetPool() == nil || len(orders) == 0 {
		return
	}
	hlog.Infof("[OrderBatch] 准备批量插入订单, 数量: %d", len(orders))
	for i, order := range orders {
		hlog.Infof("[OrderBatch] 插入订单[%d]: order_id=%v, user_id=%v, symbol=%v, side=%v, price=%v, quantity=%v", i, order.OrderID, order.UserID, order.Symbol, order.Side, order.Price, order.Quantity)
	}
	if len(orders) == 0 {
		return
	}
	// 构造原生多值INSERT语句
	query := "INSERT INTO orders (order_id, user_id, symbol, side, price, quantity, status, created_at, updated_at) VALUES "
	args := make([]interface{}, 0, len(orders)*9)
	valueStrings := make([]string, 0, len(orders))
	for i, order := range orders {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9))
		args = append(args,
			order.OrderID,
			order.UserID,
			order.Symbol,
			order.Side,
			order.Price,
			order.Quantity,
			"active",
			time.Now().UnixMilli(),
			time.Now().UnixMilli(),
		)
	}
	query += strings.Join(valueStrings, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := pg.GetPool().Exec(ctx, query, args...)
	if err != nil {
		hlog.Errorf("[OrderBatch] 批量插入订单到Postgres失败: %v", err)
	} else {
		hlog.Infof("[OrderBatch] 批量插入订单到Postgres成功, 数量: %d", len(orders))
	}
}

func (m *MatchEngine) matchWorker(symbol string, queue chan model.SubmitOrderMsg) {
	defer func() {
		if r := recover(); r != nil {
			hlog.Errorf("撮合线程panic, symbol=%s, err=%v, stack=%s", symbol, r, debug.Stack())
		}
		hlog.Infof("撮合线程退出, symbol=%s", symbol)
	}()
	orderBook := NewOrderBook(symbol)
	engineID := "node-1" // 可通过配置或环境变量设置
	hlog.Infof("撮合线程启动, symbol=%s, engine_id=%s", symbol, engineID)
	var lastSnapshot = &OrderBookSnapshot{Bids: map[string]string{}, Asks: map[string]string{}}
	for order := range queue {
		hlog.Infof("撮合订单, order_id=%s, symbol=%s, side=%s, price=%s, quantity=%s", order.OrderID, order.Symbol, order.Side, order.Price, order.Quantity)
		trades, filled := orderBook.Match(order)
		var bids, asks interface{}
		if filled {
			hlog.Infof("订单撮合成功, order_id=%s, trade_count=%d", order.OrderID, len(trades))
			for i, trade := range trades {
				trade.Symbol = symbol
				trade.Timestamp = time.Now().UnixMilli()
				trade.EngineID = engineID
				trade.TradeID = fmt.Sprintf("trade-%s-%d-%d", symbol, time.Now().UnixMilli(), i)
				// 优化推送，增加 version、server_time、node_id 字段
				//  message_id（trade_id + 时间戳 + 随机数）
				messageID := fmt.Sprintf("%s-%d-%d", trade.TradeID, trade.Timestamp, time.Now().UnixNano()%10000)
				// trace_id 可用 order_id + trade_id 拼���
				traceID := fmt.Sprintf("%s-%s", order.OrderID, trade.TradeID)
				result := map[string]interface{}{
					"channel": symbol,
					"type":    "trade",
					"data": map[string]interface{}{
						"trade_id":    trade.TradeID,
						"symbol":      trade.Symbol,
						"price":       trade.Price,
						"quantity":    trade.Quantity,
						"side":        trade.Side,
						"taker_order": trade.TakerOrderID,
						"maker_order": trade.MakerOrderID,
						"taker_user":  trade.TakerUser,
						"maker_user":  trade.MakerUser,
						"timestamp":   trade.Timestamp,
						"engine_id":   trade.EngineID,
					},
					"order_ctx": map[string]interface{}{
						"order_id": order.OrderID,
						"side":     order.Side,
						"price":    order.Price,
						"quantity": order.Quantity,
					},
					"version":     1,
					"server_time": time.Now().UnixMilli(),
					"node_id":     engineID,
					"message_id":  messageID,
					"trace_id":    traceID,
				}
				msg, err := json.Marshal(result)
				if err == nil {
					m.broadcaster(symbol, msg)
					// 单播给撮合双方
					if trade.TakerUser != "" {
						m.unicaster(trade.TakerUser, msg)
					}
					if trade.MakerUser != "" && trade.MakerUser != trade.TakerUser {
						m.unicaster(trade.MakerUser, msg)
					}
				}
				// 持久化到 Kafka
				saveTradeToKafka(trade)
				// 持久化到 PostgreSQL
				saveTradeToDB(trade)
				// 缓存成交记录到 Redis
				cacheTrade(symbol, trade, 100)
				hlog.Infof("成交回报, trade_id=%s, symbol=%s, price=%s, quantity=%s", trade.TradeID, trade.Symbol, trade.Price, trade.Quantity)
			}
		} else {
			hlog.Infof("订单未成交，推送订单簿变更, order_id=%s, symbol=%s", order.OrderID, order.Symbol)
			// 未成交，推送订单簿变更
			depth := orderBook.DepthSnapshot()
			bids, _ = depth["buys"]
			asks, _ = depth["sells"]
			version := time.Now().UnixMilli() // ��时间戳做快照版本
			messageID := fmt.Sprintf("%s-%d-%d", order.OrderID, version, time.Now().UnixNano()%10000)
			traceID := fmt.Sprintf("%s-%d", order.OrderID, version)
			result := map[string]interface{}{
				"channel": symbol,
				"type":    "depth_update",
				"data": map[string]interface{}{
					"bids":      bids,
					"asks":      asks,
					"timestamp": version,
					"version":   version,
				},
				"order_ctx": map[string]interface{}{
					"order_id": order.OrderID,
					"side":     order.Side,
					"price":    order.Price,
					"quantity": order.Quantity,
				},
				"version":     1,
				"server_time": version,
				"node_id":     engineID,
				"message_id":  messageID,
				"trace_id":    traceID,
			}
			msg, err := json.Marshal(result)
			if err == nil {
				m.broadcaster(symbol, msg)
			}

			// ��量快照推送
			delta := orderBook.DeltaSnapshot(lastSnapshot)
			if len(delta["bids_delta"].(map[string]string)) > 0 || len(delta["asks_delta"].(map[string]string)) > 0 {
				deltaResult := map[string]interface{}{
					"channel": symbol,
					"type":    "depth_delta",
					"data":    delta,
					"order_ctx": map[string]interface{}{
						"order_id": order.OrderID,
						"side":     order.Side,
						"price":    order.Price,
						"quantity": order.Quantity,
					},
					"version":     1,
					"server_time": version,
					"node_id":     engineID,
					"message_id":  messageID + "-delta",
					"trace_id":    traceID + "-delta",
				}
				deltaMsg, err := json.Marshal(deltaResult)
				if err == nil {
					m.broadcaster(symbol, deltaMsg)
				}
			}
			// 新 lastSnapshot
			// 重新生成全量 bids/asks map，增加类型断言保护，防止panic
			bidsSlice, okBids := bids.([]map[string]string)
			if !okBids || bidsSlice == nil {
				bidsSlice = []map[string]string{}
			}
			asksSlice, okAsks := asks.([]map[string]string)
			if !okAsks || asksSlice == nil {
				asksSlice = []map[string]string{}
			}
			newBids := map[string]string{}
			for _, b := range bidsSlice {
				newBids[b["price"]] = b["quantity"]
			}
			newAsks := map[string]string{}
			for _, a := range asksSlice {
				newAsks[a["price"]] = a["quantity"]
			}
			lastSnapshot = &OrderBookSnapshot{Bids: newBids, Asks: newAsks}
		}
		// 缓存订单簿快照到 Redis
		cacheOrderBookSnapshot(symbol, bids, asks)
	}
}

// OrderBook 及其相关方法已全部迁移至 orderbook.go，仅保留类型引用和必要的调用。
// 工具函数：字符串数量比较、加减
func toFloat(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

func subQty(a, b string) string {
	fa, fb := toFloat(a), toFloat(b)
	res := fa - fb
	if res < 0 {
		res = 0
	}
	return fmt.Sprintf("%.8f", res)
}
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

var (
	kafkaWriter    *kafka.Writer
	tradeBatchChan chan model.Trade
)

func InitKafkaWriter(brokers []string, topic string) {
	kafkaWriter = &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		Async: true,
	}
	tradeBatchChan = make(chan model.Trade, 10000)
	go batchKafkaWriter()
}

func batchKafkaWriter() {
	batch := make([]kafka.Message, 0, 100)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case trade := <-tradeBatchChan:
			msgBytes, err := json.Marshal(trade)
			if err != nil {
				batch = append(batch, kafka.Message{Value: msgBytes})
			}
			if len(batch) >= 100 {
				flushKafkaBatch(&batch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushKafkaBatch(&batch)
			}
		}
	}
}

func flushKafkaBatch(batch *[]kafka.Message) {
	if kafkaWriter == nil || len(*batch) == 0 {
		return
	}
	err := kafkaWriter.WriteMessages(context.Background(), (*batch)...)
	if err != nil {
		hlog.Errorf("批量写入Kafka失败: %v", err)
	}
	*batch = (*batch)[:0]
}

func saveTradeToKafka(trade model.Trade) {
	if tradeBatchChan != nil {
		tradeBatchChan <- trade
	}
}

//// K线周期定义
//var klinePeriods = []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"}
//var klinePeriodSeconds = map[string]int64{
//	"1m":  60,
//	"5m":  300,
//	"15m": 900,
//	"30m": 1800,
//	"1h":  3600,
//	"4h":  14400,
//	"1d":  86400,
//	"1w":  604800,
//	"1M":  2592000, // 30天
//}

//// ��合并写入多周期K线
//func updateKlines(db *gorm.DB, symbol, price, qty string, ts int64) {
//	for _, period := range klinePeriods {
//		bucket := ts / klinePeriodSeconds[period] * klinePeriodSeconds[period]
//		var k model.Kline
//		err := db.Where("symbol = ? AND period = ? AND timestamp = ?", symbol, period, bucket).First(&k).Error
//		if err == gorm.ErrRecordNotFound {
//			// 新K线
//			k = model.Kline{
//				symbol:      symbol,
//				Period:    period,
//				Timestamp: bucket,
//				Open:      price,
//				Close:     price,
//				High:      price,
//				Low:       price,
//				Volume:    qty,
//			}
//			db.Create(&k)
//		} else if err == nil {
//			// 更新K线
//			if price > k.High {
//				k.High = price
//			}
//			if price < k.Low {
//				k.Low = price
//			}
//			k.Close = price
//			// 累加成交��
//			if v, err := strconv.ParseFloat(k.Volume, 64); err == nil {
//				if q, err := strconv.ParseFloat(qty, 64); err == nil {
//					k.Volume = strconv.FormatFloat(v+q, 'f', -1, 64)
//				}
//			}
//			db.Save(&k)
//		}
//		// 写入Redis
//		b, _ := json.Marshal(k)
//		redisKey := "kline:" + symbol + ":" + period
//		redis.RedisClient.RPush(context.Background(), redisKey, b)
//		redis.RedisClient.LTrim(context.Background(), redisKey, -1000, -1) // 只保留最新1000条
//	}
//}

func saveTradeToDB(trade model.Trade) {
	if pg.GetPool() == nil {
		hlog.Warnf("Postgres��接池未初始化，无法持久化成交, trade_id=%s", trade.TradeID)
		return
	}
	_, err := pg.GetPool().Exec(context.Background(),
		`INSERT INTO trades (trade_id, symbol, price, quantity, timestamp, taker_order_id, maker_order_id, side, engine_id, taker_user, maker_user)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		trade.TradeID, trade.Symbol, trade.Price, trade.Quantity, trade.Timestamp, trade.TakerOrderID, trade.MakerOrderID, trade.Side, trade.EngineID, trade.TakerUser, trade.MakerUser,
	)
	if err != nil {
		hlog.Errorf("持久化成���到Postgres失败, trade_id=%s, err=%v", trade.TradeID, err)
	}
	// 新增K线聚合写入
	UpdateKlines(trade.Symbol, trade.Price, trade.Quantity, trade.Timestamp/1000)
}

// 优雅处理 bids/asks nil 为 []
func safeSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// 缓存订单簿快照到 Redis
func cacheOrderBookSnapshot(symbol string, bids, asks interface{}) {
	ctx := context.Background()
	key := "orderbook:" + symbol

	// 尝试将 bids/asks 转为 []map[string]string 类型并安全处理
	var safeBids, safeAsks interface{} = bids, asks
	if b, ok := bids.([]map[string]string); ok {
		safeBids = safeSlice(b)
	}
	if a, ok := asks.([]map[string]string); ok {
		safeAsks = safeSlice(a)
	}

	val, err := json.Marshal(map[string]interface{}{"bids": safeBids, "asks": safeAsks})
	if err == nil {
		err := redis.Client.Set(ctx, key, val, 5*time.Second).Err()
		if err != nil {
			hlog.Errorf("Redis Set 失败: %v", err)
		}
	}
}

// 缓存成交记录到 Redis List
func cacheTrade(symbol string, trade model.Trade, maxLen int64) {
	ctx := context.Background()
	key := "trades:" + symbol
	val, err := json.Marshal(trade)
	if err == nil {
		redis.Client.LPush(ctx, key, val)
		redis.Client.LTrim(ctx, key, 0, maxLen-1)
	}
}

// 缓存用户活跃订单ID到 Redis
func cacheUserActiveOrder(userID, orderID string) {
	if userID == "" || orderID == "" {
		return
	}
	ctx := context.Background()
	key := "user:active_orders:" + userID
	redis.Client.SAdd(ctx, key, orderID)
	redis.Client.Expire(ctx, key, 24*time.Hour)
}

// 从 Redis 移除用户活跃订单ID
func removeUserActiveOrder(userID, orderID string) {
	if userID == "" || orderID == "" {
		return
	}
	ctx := context.Background()
	key := "user:active_orders:" + userID
	redis.Client.SRem(ctx, key, orderID)
}

// 查询用户活跃订单ID列表
func GetUserActiveOrders(userID string) ([]string, error) {
	if userID == "" {
		return nil, nil
	}
	ctx := context.Background()
	key := "user:active_orders:" + userID
	return redis.Client.SMembers(ctx, key).Result()
}

// OrderStatus 订单状态常量
type OrderStatus string

const (
	OrderStatusSubmitted  OrderStatus = "submitted"
	OrderStatusPartFilled OrderStatus = "part_filled"
	OrderStatusFilled     OrderStatus = "filled"
	OrderStatusCancelled  OrderStatus = "cancelled"
	OrderStatusFailed     OrderStatus = "failed"
)

// EngineOrderMsg 订单结构体，包含状态、时间、用户等
// 可扩展���多字段
// 用于撮合引擎内部流转
type EngineOrderMsg struct {
	OrderID   string      `json:"order_id"`
	symbol    string      `json:"symbol"`
	Side      string      `json:"side"`
	Price     string      `json:"price"`
	Quantity  string      `json:"quantity"`
	UserID    string      `json:"user_id,omitempty"`
	Status    OrderStatus `json:"status"`
	CreatedAt int64       `json:"created_at"`
	UpdatedAt int64       `json:"updated_at"`
	FilledQty string      `json:"filled_qty"`
	ErrMsg    string      `json:"err_msg,omitempty"`
}

// 判断本节点是否负责该symbol
// 建议迁移到 util.go 工具函数文件���
// func IsLocalMatchEngine(symbol string) bool {
// 	if consulHelper == nil {
// 		return true // 未启用Consul则全部��地处理
// 	}
// 	return isLocalSymbol(symbol)
// }

// isLocalSymbol 判断symbol是否属于本节点
// ���议迁移到 util.go 工具函数文件中
// func isLocalSymbol(symbol string) bool {
// 	symbols := getLocalsymbols(cfg)
// 	for _, p := range symbols {
// 		if p == symbol {
// 			return true
// 		}
// 	}
// 	return false
// }

// ForwardOrderToMatchEngine 转发订单到目��撮合节点（HTTP示例）
func ForwardOrderToMatchEngine(symbol string, data []byte) error {
	if consulHelper == nil {
		return fmt.Errorf("consul not initialized")
	}
	nodes, err := consulHelper.DiscoverMatchEngine(symbol)
	if err != nil || len(nodes) == 0 {
		return fmt.Errorf("no match engine found for symbol %s", symbol)
	}
	// 随机选择一个节点实现负载均衡
	idx := rand.Intn(len(nodes))
	url := fmt.Sprintf("http://%s:%d/submit_order", nodes[idx].Address, nodes[idx].Port)
	resp, err := httpPost(url, data)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("remote match engine error: %s", resp.Status)
	}
	return nil
}

// httpPost 简单HTTP POST封装
func httpPost(url string, data []byte) (*http.Response, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}

// GetOrderByID 查���单个订单（优先查数据库）
func (m *MatchEngine) GetOrderByID(orderID string) (model.SubmitOrderMsg, error) {
	order, err := GetOrderByID(orderID)
	if err != nil {
		return model.SubmitOrderMsg{}, err
	}
	return model.SubmitOrderMsg{
		OrderID:  order.OrderID,
		UserID:   order.UserID,
		Symbol:   order.Symbol,
		Side:     order.Side,
		Price:    order.Price,
		Quantity: order.Quantity,
	}, nil
}

// ListOrders 查询订单列表（优先查数据库）
func (m *MatchEngine) ListOrders(userID, status string) ([]model.SubmitOrderMsg, error) {
	rows, err := ListOrders(userID, status)
	if err != nil {
		return nil, err
	}
	var result []model.SubmitOrderMsg
	for _, row := range rows {
		result = append(result, model.SubmitOrderMsg{
			OrderID:  row.OrderID,
			UserID:   row.UserID,
			Symbol:   row.Symbol,
			Side:     row.Side,
			Price:    row.Price,
			Quantity: row.Quantity,
		})
	}
	return result, nil
}

// CancelOrder 取消订单（持久化状态到数据库）
func (m *MatchEngine) CancelOrder(orderID, userID string) (string, error) {
	order, err := m.GetOrderByID(orderID)
	if err != nil || order.UserID != userID {
		return "not_found", err
	}
	err = UpdateOrderStatus(orderID, "cancelled")
	if err != nil {
		return "error", err
	}
	return "cancelled", nil
}

// 在撮合成功、订单状态变化等需要单播时调用 m.unicaster(userID, msg)
