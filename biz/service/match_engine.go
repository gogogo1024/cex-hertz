package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	kafkaDal "github.com/gogogo1024/cex-hertz-backend/biz/dal/kafka"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/redis"
	rocksdbCompensate "github.com/gogogo1024/cex-hertz-backend/biz/dal/rocksdb"
	"github.com/gogogo1024/cex-hertz-backend/biz/engine"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/conf"
	"github.com/gogogo1024/cex-hertz-backend/util"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/segmentio/kafka-go"
)

// 类型定义区
type Engine interface {
	SubmitOrder(order model.SubmitOrderMsg)
	// ...其他方法...
}

type MatchEngine struct {
	orderQueues sync.Map // key: symbol, value: chan model.SubmitOrderMsg
	broadcaster engine.Broadcaster
	unicaster   engine.Unicaster
}

type PartitionAwareMatchEngine struct {
	*MatchEngine
	pm        *PartitionManager
	localAddr string
	ctx       context.Context
	cancel    context.CancelFunc
}

type OrderBookUpdateParams struct {
	OrderBook    *OrderBook
	Order        model.SubmitOrderMsg
	Symbol       string
	EngineID     string
	LastSnapshot *OrderBookSnapshot
	Bids         *interface{}
	Asks         *interface{}
	BatchResults *[]struct {
		msgType  string
		symbol   string
		orderID  string
		price    string
		quantity string
		status   string
		ts       int64
	}
}

// 全局变量区
var (
	orderStore        sync.Map // orderID -> model.SubmitOrderMsg
	userOrderMap      sync.Map // userID -> []orderID
	orderStatus       sync.Map // orderID -> status
	userOrderMuMap    sync.Map // userID -> *sync.Mutex 获取用户订单互斥锁，防止并发append
	consulHelper      *ConsulHelper
	MatchResultPusher func(msgType, symbol, orderID, price, quantity, status string, ts int64)
	// Removed duplicate variable declarations
	// 已清理所有重复声明
)

// 构造函数区
func NewMatchEngine(broadcaster engine.Broadcaster, unicaster engine.Unicaster) *MatchEngine {
	return &MatchEngine{
		broadcaster: broadcaster,
		unicaster:   unicaster,
		orderQueues: sync.Map{},
	}
}

// SubmitOrder 持久化到Kafka，由独立消费者批量入库
// 核心方法区
func (m *MatchEngine) SubmitOrder(order model.SubmitOrderMsg) {
	id, err := util.GenerateOrderID()
	if err != nil {
		hlog.Errorf("生成订单ID失败: %v", err)
		return
	}
	order.OrderID = fmt.Sprintf("%d", id)

	hlog.Infof("SubmitOrder called, order_id=%s, symbol=%s, side=%s, price=%s, quantity=%s", order.OrderID, order.Symbol, order.Side, order.Price, order.Quantity)
	queueAny, ok := m.orderQueues.Load(order.Symbol)
	var queue chan model.SubmitOrderMsg
	if !ok {
		newQueue := make(chan model.SubmitOrderMsg, 10000)
		actual, loaded := m.orderQueues.LoadOrStore(order.Symbol, newQueue)
		if !loaded {
			queue = newQueue
			go m.matchWorker(order.Symbol, queue)
		} else {
			queue = actual.(chan model.SubmitOrderMsg)
		}
	} else {
		queue = queueAny.(chan model.SubmitOrderMsg)
	}
	orderStore.Store(order.OrderID, order)
	userOrderMu := getUserOrderMutex(order.UserID)
	userOrderMu.Lock()
	val, _ := userOrderMap.LoadOrStore(order.UserID, []string{})
	orderIDs := val.([]string)
	orderIDs = append(orderIDs, order.OrderID)
	userOrderMap.Store(order.UserID, orderIDs)
	userOrderMu.Unlock()
	orderStatus.Store(order.OrderID, "active")
	saveOrderToKafka(order)
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
				// 设置 symbol 作为 Kafka 消息 key，实现分区路由
				batch = append(batch, kafka.Message{Key: []byte(order.Symbol), Value: msgBytes})
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

// flushOrderKafkaBatch，带本地补偿逻辑
func flushOrderKafkaBatch(batch *[]kafka.Message) {
	if len(*batch) == 0 {
		hlog.Infof("[OrderKafkaBatch] flushOrderKafkaBatch被调用,但batch为空")
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
		// 失败时本地补偿
		for _, msg := range *batch {
			var order model.SubmitOrderMsg
			if err := json.Unmarshal(msg.Value, &order); err == nil {
				_ = rocksdbCompensate.SaveOrderCompensate(order.OrderID, order)
			}
		}
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
var orderKafkaConsumerWg sync.WaitGroup

func StartOrderKafkaConsumer(topic string) {
	orderKafkaConsumerClose = make(chan struct{})
	brokers := conf.GetConf().Kafka.Brokers
	consumerNum := runtime.NumCPU()
	for i := 0; i < consumerNum; i++ {
		orderKafkaConsumerWg.Add(1)
		go orderKafkaConsumerWorker(i, brokers, topic)
	}
}

func StopOrderKafkaConsumer() {
	close(orderKafkaConsumerClose)
	orderKafkaConsumerWg.Wait()
}

func StopOrderKafkaConsumerWithTimeout(timeout time.Duration) {
	if orderKafkaConsumerClose == nil {
		return
	}
	close(orderKafkaConsumerClose)
	c := make(chan struct{})
	go func() {
		orderKafkaConsumerWg.Wait()
		close(c)
	}()
	select {
	case <-c:
		// 正常退出
	case <-time.After(timeout):
		hlog.Warnf("StopOrderKafkaConsumer超时退出，可能有未处理完的消息")
	}
}

func orderKafkaConsumerWorker(idx int, brokers []string, topic string) {
	defer func() {
		if r := recover(); r != nil {
			hlog.Errorf("[OrderKafkaConsumer-%d] panic: %v\n%s", idx, r, debug.Stack())
		}
		orderKafkaConsumerWg.Done()
	}()
	r := initOrderKafkaReader(brokers, topic)
	batch := make([]model.SubmitOrderMsg, 0, 50000)
	var batchWait time.Duration = 1 * time.Second
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	hlog.Infof("[OrderKafkaConsumer-%d] Kafka Reader初始化: topic=%s, groupID=%s, brokers=%v", idx, topic, "order-db-writer", brokers)
	for {
		select {
		case <-orderKafkaConsumerClose:
			if len(batch) > 0 {
				batchInsertOrders(batch)
				batch = batch[:0]
				// 超时关闭时落盘补偿
				saveBatchToRedis(idx, batch)
			}
			return
		case <-ticker.C:
			batchWait, _ = adjustBatchParams(r)
			msgBatch := consumeOrderMessages(r, batchWait)
			if len(msgBatch) > 0 {
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
		return 100 * time.Millisecond, 0
	} else if lag > 5000 {
		return 500 * time.Millisecond, 0
	}
	return 1 * time.Second, 0
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
	if pg.GetPool() == nil || len(orders) == 0 {
		return
	}
	query, args := buildOrderInsertQuery(orders)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := pg.GetPool().Exec(ctx, query, args...)
	if err != nil {
		handleOrderInsertError(err, orders)
	} else {
		hlog.Infof("[OrderBatch] 批量插入订单到Postgres成功, 数量: %d", len(orders))
	}
}

func buildOrderInsertQuery(orders []model.SubmitOrderMsg) (string, []interface{}) {
	query := "INSERT INTO orders (order_id, user_id, symbol, side, price, quantity, status, created_at, updated_at) VALUES "
	args := make([]interface{}, 0, len(orders)*9)
	valueStrings := make([]string, 0, len(orders))
	now := time.Now().UnixMilli()
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
			now,
			now,
		)
	}
	query += strings.Join(valueStrings, ",")
	query += " ON CONFLICT(order_id) DO NOTHING"
	return query, args
}

func handleOrderInsertError(err error, orders []model.SubmitOrderMsg) {
	hlog.Errorf("[OrderBatch] 批量插入订单到Postgres失败: %v", err)
	kafkaMsgs := buildOrderKafkaMessages(orders)
	if len(kafkaMsgs) > 0 {
		retryOrderKafkaBatch(kafkaMsgs)
	}
}

func buildOrderKafkaMessages(orders []model.SubmitOrderMsg) []kafka.Message {
	var kafkaMsgs []kafka.Message
	for _, order := range orders {
		msgBytes, err := json.Marshal(order)
		if err != nil {
			hlog.Errorf("[OrderBatch] 订单序列化失败, orderID=%s, err=%v", order.OrderID, err)
			continue
		}
		kafkaMsgs = append(kafkaMsgs, kafka.Message{Key: []byte(order.Symbol), Value: msgBytes})
	}
	return kafkaMsgs
}

func retryOrderKafkaBatch(kafkaMsgs []kafka.Message) {
	writer := kafkaDal.GetWriter(orderKafkaTopic)
	if writer == nil {
		hlog.Errorf("[OrderBatch] Kafka writer未初始化，无法重投, orders count=%d", len(kafkaMsgs))
		return
	}
	const maxBatchBytes = 10 * 1024 * 1024 // 10MB
	const maxBatchCount = 2000
	batch := make([]kafka.Message, 0, maxBatchCount)
	batchBytes := 0
	for _, msg := range kafkaMsgs {
		msgBytes := len(msg.Value) + len(msg.Key)
		if len(batch) >= maxBatchCount || batchBytes+msgBytes > maxBatchBytes {
			writeKafkaBatch(writer, batch)
			batch = batch[:0]
			batchBytes = 0
		}
		batch = append(batch, msg)
		batchBytes += msgBytes
	}
	if len(batch) > 0 {
		writeKafkaBatch(writer, batch)
	}
}

func writeKafkaBatch(writer *kafka.Writer, batch []kafka.Message) {
	err := writer.WriteMessages(context.Background(), batch...)
	if err != nil {
		hlog.Errorf("[OrderBatch] 批量重投Kafka失败, count=%d, err=%v", len(batch), err)
	}
}
func (m *MatchEngine) processOrderBookUpdate(params OrderBookUpdateParams) {
	hlog.Infof("订单未成交，推送订单簿变更, order_id=%s, symbol=%s", params.Order.OrderID, params.Order.Symbol)
	depth := params.OrderBook.DepthSnapshot()
	*params.Bids = depth["buys"]
	*params.Asks = depth["sells"]
	version := time.Now().UnixMilli()
	messageID := fmt.Sprintf("%s-%d-%d", params.Order.OrderID, version, time.Now().UnixNano()%10000)
	traceID := fmt.Sprintf("%s-%d", params.Order.OrderID, version)
	result := map[string]interface{}{
		"channel": params.Symbol,
		"msgType": "depth_update",
		"data": map[string]interface{}{
			"bids":      *params.Bids,
			"asks":      *params.Asks,
			"timestamp": version,
			"version":   version,
		},
		"order_ctx": map[string]interface{}{
			"order_id": params.Order.OrderID,
			"side":     params.Order.Side,
			"price":    params.Order.Price,
			"quantity": params.Order.Quantity,
		},
		"version":     1,
		"server_time": version,
		"node_id":     params.EngineID,
		"message_id":  messageID,
		"trace_id":    traceID,
	}
	msg, err := json.Marshal(result)
	if err == nil {
		m.broadcaster(params.Symbol, msg)
	}
	delta := params.OrderBook.DeltaSnapshot(params.LastSnapshot)
	if len(delta["bids_delta"].(map[string]string)) > 0 || len(delta["asks_delta"].(map[string]string)) > 0 {
		deltaResult := map[string]interface{}{
			"channel": params.Symbol,
			"msgType": "depth_delta",
			"data":    delta,
			"order_ctx": map[string]interface{}{
				"order_id": params.Order.OrderID,
				"side":     params.Order.Side,
				"price":    params.Order.Price,
				"quantity": params.Order.Quantity,
			},
			"version":     1,
			"server_time": version,
			"node_id":     params.EngineID,
			"message_id":  messageID + "-delta",
			"trace_id":    traceID + "-delta",
		}
		deltaMsg, err := json.Marshal(deltaResult)
		if err == nil {
			m.broadcaster(params.Symbol, deltaMsg)
		}
	}
	*params.BatchResults = append(*params.BatchResults, struct {
		msgType  string
		symbol   string
		orderID  string
		price    string
		quantity string
		status   string
		ts       int64
	}{
		msgType:  "active",
		symbol:   params.Order.Symbol,
		orderID:  params.Order.OrderID,
		price:    params.Order.Price,
		quantity: params.Order.Quantity,
		status:   "active",
		ts:       time.Now().UnixMilli(),
	})
}

var tradeBatchForDB []model.Trade

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
	var batchResults []struct {
		msgType  string
		symbol   string
		orderID  string
		price    string
		quantity string
		status   string
		ts       int64
	}
	userMsgMap := make(map[string][][]byte) // userID -> 多条消息

	for order := range queue {
		hlog.Infof("撮合订单, order_id=%s, symbol=%s, side=%s, price=%s, quantity=%s", order.OrderID, order.Symbol, order.Side, order.Price, order.Quantity)
		trades, filled := orderBook.Match(order)
		var bids, asks interface{}
		if filled {
			m.processTrades(trades, symbol, engineID, order, userMsgMap, &batchResults)
		} else {
			m.processOrderBookUpdate(OrderBookUpdateParams{
				OrderBook:    orderBook,
				Order:        order,
				Symbol:       symbol,
				EngineID:     engineID,
				LastSnapshot: lastSnapshot,
				Bids:         &bids,
				Asks:         &asks,
				BatchResults: &batchResults,
			})
			lastSnapshot = m.updateOrderBookSnapshot(bids, asks)
			cacheOrderBookSnapshot(symbol, bids, asks)
		}
		if len(tradeBatchForDB) > 0 {
			batchInsertTrades(tradeBatchForDB)
			tradeBatchForDB = tradeBatchForDB[:0]
		}
	}
	m.batchUnicaster(userMsgMap)
	m.pushMatchResults(batchResults)
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

// Helper: build trade message
func (m *MatchEngine) buildTradeMsg(trade model.Trade, order model.SubmitOrderMsg, engineID, messageID, traceID string) []byte {
	buf := engine.BufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(`{"type":"trade","symbol":"`)
	buf.WriteString(trade.Symbol)
	buf.WriteString(`","data":{`)
	buf.WriteString(`"trade_id":"`)
	buf.WriteString(trade.TradeID)
	buf.WriteString(`","price":"`)
	buf.WriteString(trade.Price)
	buf.WriteString(`","quantity":"`)
	buf.WriteString(trade.Quantity)
	buf.WriteString(`","side":"`)
	buf.WriteString(trade.Side)
	buf.WriteString(`","taker_order_id":"`)
	buf.WriteString(trade.TakerOrderID)
	buf.WriteString(`","maker_order_id":"`)
	buf.WriteString(trade.MakerOrderID)
	buf.WriteString(`","taker_user":"`)
	buf.WriteString(trade.TakerUser)
	buf.WriteString(`","maker_user":"`)
	buf.WriteString(trade.MakerUser)
	buf.WriteString(`","timestamp":`)
	buf.WriteString(fmt.Sprintf("%d", trade.Timestamp))
	buf.WriteString(`,"engine_id":"`)
	buf.WriteString(trade.EngineID)
	buf.WriteString(`"},"order_ctx":{`)
	buf.WriteString(`"order_id":"`)
	buf.WriteString(order.OrderID)
	buf.WriteString(`","side":"`)
	buf.WriteString(order.Side)
	buf.WriteString(`","price":"`)
	buf.WriteString(order.Price)
	buf.WriteString(`","quantity":"`)
	buf.WriteString(order.Quantity)
	buf.WriteString(`"},"version":1,"server_time":`)
	buf.WriteString(fmt.Sprintf("%d", time.Now().UnixMilli()))
	buf.WriteString(`,"node_id":"`)
	buf.WriteString(engineID)
	buf.WriteString(`","message_id":"`)
	buf.WriteString(messageID)
	buf.WriteString(`","trace_id":"`)
	buf.WriteString(traceID)
	buf.WriteString(`"}`)
	msgPtr := engine.MsgBytePool.Get().(*[]byte)
	if cap(*msgPtr) < buf.Len() {
		*msgPtr = make([]byte, buf.Len())
	}
	*msgPtr = (*msgPtr)[:buf.Len()]
	copy(*msgPtr, buf.Bytes())
	engine.BufferPool.Put(buf)
	return *msgPtr
}

// Helper: process trades
func (m *MatchEngine) processTrades(trades []model.Trade, symbol, engineID string, order model.SubmitOrderMsg, userMsgMap map[string][][]byte, batchResults *[]struct {
	msgType  string
	symbol   string
	orderID  string
	price    string
	quantity string
	status   string
	ts       int64
}) {
	for i, trade := range trades {
		trade.Symbol = symbol
		trade.Timestamp = time.Now().UnixMilli()
		trade.EngineID = engineID
		trade.TradeID = fmt.Sprintf("trade-%s-%d-%d", symbol, time.Now().UnixMilli(), i)
		messageID := fmt.Sprintf("%s-%d-%d", trade.TradeID, trade.Timestamp, time.Now().UnixNano()%10000)
		traceID := fmt.Sprintf("%s-%s", order.OrderID, trade.TradeID)
		msg := m.buildTradeMsg(trade, order, engineID, messageID, traceID)
		_ = engine.BroadcastPool.Submit(func() {
			m.broadcaster(symbol, msg)
		})
		if trade.TakerUser != "" {
			userMsgMap[trade.TakerUser] = append(userMsgMap[trade.TakerUser], msg)
		}
		if trade.MakerUser != "" && trade.MakerUser != trade.TakerUser {
			userMsgMap[trade.MakerUser] = append(userMsgMap[trade.MakerUser], msg)
		}
		engine.MsgBytePool.Put(msg)
		go saveTradeToKafka(trade)
		saveTradeToDB(trade)
		cacheTrade(symbol, trade, 100)
		hlog.Infof("成交回报, trade_id=%s, symbol=%s, price=%s, quantity=%s", trade.TradeID, trade.Symbol, trade.Price, trade.Quantity)
		*batchResults = append(*batchResults, struct {
			msgType  string
			symbol   string
			orderID  string
			price    string
			quantity string
			status   string
			ts       int64
		}{
			msgType:  "trade",
			symbol:   trade.Symbol,
			orderID:  trade.TakerOrderID,
			price:    trade.Price,
			quantity: trade.Quantity,
			status:   "filled",
			ts:       trade.Timestamp,
		})
		*batchResults = append(*batchResults, struct {
			msgType  string
			symbol   string
			orderID  string
			price    string
			quantity string
			status   string
			ts       int64
		}{
			msgType:  "trade",
			symbol:   trade.Symbol,
			orderID:  trade.MakerOrderID,
			price:    trade.Price,
			quantity: trade.Quantity,
			status:   "filled",
			ts:       trade.Timestamp,
		})
		tradeBatchForDB = append(tradeBatchForDB, trade)

		// 持仓自动调整（买方买入，卖方卖出）
		go func(trade model.Trade) {
			if err := BuyPosition(trade.TakerUser, trade.Symbol, trade.Quantity, trade.Price); err != nil {
				hlog.Errorf("BuyPosition失败: user=%s, symbol=%s, qty=%s, price=%s, err=%v", trade.TakerUser, trade.Symbol, trade.Quantity, trade.Price, err)
			}
			if err := SellPosition(trade.MakerUser, trade.Symbol, trade.Quantity); err != nil {
				hlog.Errorf("SellPosition失败: user=%s, symbol=%s, qty=%s, err=%v", trade.MakerUser, trade.Symbol, trade.Quantity, err)
			}
		}(trade)
	}
}

// Helper: update order book snapshot
func (m *MatchEngine) updateOrderBookSnapshot(bids, asks interface{}) *OrderBookSnapshot {
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
	return &OrderBookSnapshot{Bids: newBids, Asks: newAsks}
}

// Helper: batch unicaster
func (m *MatchEngine) batchUnicaster(userMsgMap map[string][][]byte) {
	for userID, msgs := range userMsgMap {
		if len(msgs) == 1 {
			m.unicaster(userID, msgs[0])
		} else if len(msgs) > 1 {
			batchMsg, err := json.Marshal(msgs)
			if err == nil {
				m.unicaster(userID, batchMsg)
			}
		}
	}
}

// Helper: push match results
func (m *MatchEngine) pushMatchResults(batchResults []struct {
	msgType  string
	symbol   string
	orderID  string
	price    string
	quantity string
	status   string
	ts       int64
}) {
	if MatchResultPusher != nil {
		for _, r := range batchResults {
			MatchResultPusher(r.msgType, r.symbol, r.orderID, r.price, r.quantity, r.status, r.ts)
		}
	}
}

// PartitionAwareMatchEngine 支持动态感知分区变更的撮合引擎
// localAddr: 本 worker 节点地址
// pm: PartitionManager

// ...existing code...

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

// closeOrderQueue 优雅关闭 symbol 的撮合队列
func (pa *PartitionAwareMatchEngine) closeOrderQueue(symbol string) {
	q, ok := pa.orderQueues.Load(symbol)
	if ok {
		ch := q.(chan model.SubmitOrderMsg)
		close(ch)
	}
}

// getOrderQueue 获取 symbol 的撮合队列
func (pa *PartitionAwareMatchEngine) getOrderQueue(symbol string) chan model.SubmitOrderMsg {
	q, _ := pa.orderQueues.Load(symbol)
	return q.(chan model.SubmitOrderMsg)
}

// NewPartitionAwareMatchEngine 创建支持动态分区的撮合引擎
func NewPartitionAwareMatchEngine(pm *PartitionManager, localAddr string, broadcaster engine.Broadcaster, unicaster engine.Unicaster) *PartitionAwareMatchEngine {
	ctx, cancel := context.WithCancel(context.Background())
	pa := &PartitionAwareMatchEngine{
		MatchEngine: NewMatchEngine(broadcaster, unicaster),
		pm:          pm,
		localAddr:   localAddr,
		ctx:         ctx,
		cancel:      cancel,
	}
	go pa.watchPartitionChange()
	return pa
}

// watchPartitionChange 持续监听分区表变更，动态订阅/退订 symbol
func (pa *PartitionAwareMatchEngine) watchPartitionChange() {
	pa.pm.WatchPartitionTable(pa.ctx)
	var lastSymbols map[string]struct{}
	for {
		select {
		case <-pa.ctx.Done():
			return
		default:
			pt := pa.pm.GetPartitionTable()
			mySymbols := make(map[string]struct{})
			for _, partition := range pt.Partitions {
				for _, addr := range partition.Workers {
					if addr == pa.localAddr {
						for _, symbol := range partition.Symbols {
							mySymbols[symbol] = struct{}{}
						}
					}
				}
			}
			// 订阅新 symbol
			for symbol := range mySymbols {
				if lastSymbols == nil || lastSymbols[symbol] == struct{}{} {
					continue
				}
				// 新 symbol，初始化撮合队列
				_, loaded := pa.orderQueues.LoadOrStore(symbol, make(chan model.SubmitOrderMsg, 10000))
				if !loaded {
					go pa.matchWorker(symbol, pa.getOrderQueue(symbol))
				}
			}
			// 退订不再负责的 symbol
			if lastSymbols != nil {
				for symbol := range lastSymbols {
					if _, ok := mySymbols[symbol]; !ok {
						pa.closeOrderQueue(symbol)
						pa.orderQueues.Delete(symbol)
					}
				}
			}
			lastSymbols = mySymbols
		}
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

// ForwardOrderToMatchEngine 转发订单到目的撮合节点（HTTP示例）
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
func saveTradeToDB(trade model.Trade) {
	if pg.GetPool() == nil {
		hlog.Warnf("Postgres连接池未初始化，无法持久化成交, trade_id=%s", trade.TradeID)
		return
	}
	_, err := pg.GetPool().Exec(context.Background(),
		`INSERT INTO trades (trade_id, symbol, price, quantity, timestamp, taker_order_id, maker_order_id, side, engine_id, taker_user, maker_user)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		trade.TradeID, trade.Symbol, trade.Price, trade.Quantity, trade.Timestamp, trade.TakerOrderID, trade.MakerOrderID, trade.Side, trade.EngineID, trade.TakerUser, trade.MakerUser,
	)
	if err != nil {
		hlog.Errorf("持久化到Postgres失败, trade_id=%s, err=%v", trade.TradeID, err)
	}
	// 新增K线聚合写入
	UpdateKlines(trade.Symbol, trade.Price, trade.Quantity, trade.Timestamp/1000)
}

// 新增批量入库方法
func batchInsertTrades(trades []model.Trade) {
	if pg.GetPool() == nil || len(trades) == 0 {
		return
	}
	query := "INSERT INTO trades (trade_id, symbol, price, quantity, timestamp, taker_order_id, maker_order_id, side, engine_id, taker_user, maker_user) VALUES "
	args := make([]interface{}, 0, len(trades)*11)
	valueStrings := make([]string, 0, len(trades))
	for i, trade := range trades {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
		args = append(args,
			trade.TradeID,
			trade.Symbol,
			trade.Price,
			trade.Quantity,
			trade.Timestamp,
			trade.TakerOrderID,
			trade.MakerOrderID,
			trade.Side,
			trade.EngineID,
			trade.TakerUser,
			trade.MakerUser,
		)
	}
	query += strings.Join(valueStrings, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := pg.GetPool().Exec(ctx, query, args...)
	if err != nil {
		hlog.Errorf("批量插入trades到Postgres失败: %v", err)
	}
}

// RecoverCompensateOrders 自动恢复本地补偿订单并重试写入Kafka
func RecoverCompensateOrders() {
	orders, err := rocksdbCompensate.GetAllOrderCompensates()
	if err != nil {
		hlog.Errorf("[CompensateRecover] 获取本地补偿订单失败: %v", err)
		return
	}
	for orderID, comp := range orders {
		if comp.RetryCount >= rocksdbCompensate.MaxRetryCount {
			hlog.Errorf("[CompensateRecover] 补偿订单重试次数超限, orderID=%s, retryCount=%d，请人工介入", orderID, comp.RetryCount)
			continue
		}
		var order model.SubmitOrderMsg
		if err := json.Unmarshal(comp.OrderJSON, &order); err != nil {
			hlog.Errorf("[CompensateRecover] 补偿订单反序列化失败, orderID=%s, err=%v", orderID, err)
			continue
		}
		err := tryWriteOrderToKafka(order)
		if err == nil {
			if err := rocksdbCompensate.DeleteOrderCompensate(orderID); err != nil {
				hlog.Errorf("[CompensateRecover] 删除本地补偿失败, orderID=%s, err=%v", orderID, err)
			}
		} else {
			hlog.Errorf("[CompensateRecover] 重试写入Kafka失败, orderID=%s, err=%v", orderID, err)
			// 写入失败，更新重试次数和时间戳
			if err := rocksdbCompensate.UpdateOrderCompensateRetry(orderID, comp); err != nil {
				hlog.Errorf("[CompensateRecover] 更新补偿重试信息失败, orderID=%s, err=%v", orderID, err)
			}
		}
	}
}

// 封装Kafka写入，失败时写入本地补偿
func tryWriteOrderToKafka(order model.SubmitOrderMsg) error {
	msgBytes, err := json.Marshal(order)
	if err != nil {
		return err
	}
	writer := kafkaDal.GetWriter(orderKafkaTopic)
	if writer == nil {
		hlog.Errorf("Kafka writer未初始化")
		return fmt.Errorf("kafka writer未初始化")
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{Key: []byte(order.Symbol), Value: msgBytes})
	if err != nil {
		// 写入失败，落盘补偿
		err2 := rocksdbCompensate.SaveOrderCompensate(order.OrderID, order)
		if err2 != nil {
			hlog.Errorf("[Compensate] 本地补偿写入失败, orderID=%s, err=%v", order.OrderID, err2)
		}
		return err
	}
	return nil
}

func getUserOrderMutex(userID string) *sync.Mutex {
	muIface, _ := userOrderMuMap.LoadOrStore(userID, &sync.Mutex{})
	return muIface.(*sync.Mutex)
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

// 批量补偿数据写入Redis（用于Kafka消费超时等场景）
func saveBatchToRedis(workerID int, batch []model.SubmitOrderMsg) {
	if len(batch) == 0 {
		return
	}
	ctx := context.Background()
	key := fmt.Sprintf("order_compensate_batch:%d", workerID)
	val, err := json.Marshal(batch)
	if err != nil {
		hlog.Errorf("[Compensate] worker-%d batch序列化失败: %v", workerID, err)
		return
	}
	maxRetry := 3
	for i := 0; i < maxRetry; i++ {
		err = redis.Client.Set(ctx, key, val, 24*time.Hour).Err()
		if err == nil {
			hlog.Infof("[Compensate] worker-%d batch已写入Redis补偿, 数量=%d", workerID, len(batch))
			return
		}
		hlog.Errorf("[Compensate] worker-%d batch写入Redis失败(第%d次): %v", workerID, i+1, err)
		time.Sleep(500 * time.Millisecond)
	}
	hlog.Errorf("[Compensate] worker-%d batch写入Redis最终失败，需人工介入! 失败订单ID: %v", workerID, extractOrderIDs(batch))
}

func extractOrderIDs(batch []model.SubmitOrderMsg) []string {
	ids := make([]string, 0, len(batch))
	for _, o := range batch {
		ids = append(ids, o.OrderID)
	}
	return ids
}
