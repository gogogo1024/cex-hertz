package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"net/http"
	"sync"
	"time"

	"cex-hertz/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
)

// 撮合引擎接口
// 实际业务可扩展为分 pair 的撮合线程池

type MatchEngine struct {
	// 每个交易对一个撮合队列
	orderQueues map[string]chan SubmitOrderMsg
	mu          sync.RWMutex
}

// SubmitOrderMsg 订单结构体（与 handler 保持一致）
type SubmitOrderMsg struct {
	OrderID  string
	Pair     string
	Side     string
	Price    string
	Quantity string
	UserID   string // 新增 UserID 字段
}

var (
	Engine       = NewMatchEngine()
	consulHelper *ConsulHelper
)

func NewMatchEngine() *MatchEngine {
	return &MatchEngine{
		orderQueues: make(map[string]chan SubmitOrderMsg),
	}
}

// InitMatchEngine 初始化撮合引擎并注册到Consul
func InitMatchEngine(consulAddr, nodeID string, pairs []string, port int) error {
	h, err := NewConsulHelper(consulAddr)
	if err != nil {
		return err
	}
	consulHelper = h
	err = consulHelper.RegisterMatchEngine(nodeID, pairs, port)
	if err != nil {
		return err
	}
	hlog.Infof("MatchEngine节点已注册到Consul, nodeID=%s, pairs=%v, port=%d", nodeID, pairs, port)
	return nil
}

// SubmitOrder 按 pair 分发订单到对应撮合队列
func (m *MatchEngine) SubmitOrder(order SubmitOrderMsg) {
	hlog.Infof("SubmitOrder called, order_id=%s, pair=%s, side=%s, price=%s, quantity=%s", order.OrderID, order.Pair, order.Side, order.Price, order.Quantity)
	m.mu.Lock()
	queue, ok := m.orderQueues[order.Pair]
	if !ok {
		queue = make(chan SubmitOrderMsg, 10000) // 每个交易对一个队列
		m.orderQueues[order.Pair] = queue
		// 启动独立撮合 worker goroutine
		go m.matchWorker(order.Pair, queue)
	}
	m.mu.Unlock()
	// 缓存用户活跃订单ID
	if order.UserID != "" {
		cacheUserActiveOrder(order.UserID, order.OrderID)
	}
	queue <- order
}

// 每个交易对独立撮合 worker
func (m *MatchEngine) matchWorker(pair string, queue chan SubmitOrderMsg) {
	orderBook := NewOrderBook(pair)
	engineID := "node-1" // 可通过配置或环境变量设置
	hlog.Infof("撮合线程启动, pair=%s, engine_id=%s", pair, engineID)
	var lastSnapshot *OrderBookSnapshot = &OrderBookSnapshot{Bids: map[string]string{}, Asks: map[string]string{}}
	for order := range queue {
		hlog.Infof("撮���订单, order_id=%s, pair=%s, side=%s, price=%s, quantity=%s", order.OrderID, order.Pair, order.Side, order.Price, order.Quantity)
		trades, filled := orderBook.Match(order)
		var bids, asks interface{}
		if filled {
			hlog.Infof("订单撮合成功, order_id=%s, trade_count=%d", order.OrderID, len(trades))
			for i, trade := range trades {
				trade.Pair = pair
				trade.Timestamp = time.Now().UnixMilli()
				trade.EngineID = engineID
				trade.TradeID = fmt.Sprintf("trade-%s-%d-%d", pair, time.Now().UnixMilli(), i)
				// 优化推送内容，增加 version、server_time、node_id 字段
				// 生成唯一 message_id（trade_id + 时间戳 + 随机数）
				messageID := fmt.Sprintf("%s-%d-%d", trade.TradeID, trade.Timestamp, time.Now().UnixNano()%10000)
				// trace_id 可用 order_id + trade_id 拼接
				traceID := fmt.Sprintf("%s-%s", order.OrderID, trade.TradeID)
				result := map[string]interface{}{
					"channel": pair,
					"type":    "trade",
					"data": map[string]interface{}{
						"trade_id":    trade.TradeID,
						"pair":        trade.Pair,
						"price":       trade.Price,
						"quantity":    trade.Quantity,
						"side":        trade.Side,
						"taker_order": trade.TakerOrder,
						"maker_order": trade.MakerOrder,
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
					Broadcast(pair, msg)
				}
				// 持久化到 Kafka
				saveTradeToKafka(trade)
				// 持久化到 PostgreSQL
				saveTradeToDB(trade)
				// 缓存成交记录到 Redis
				cacheTrade(pair, trade, 100)
				hlog.Infof("成交回报, trade_id=%s, pair=%s, price=%s, quantity=%s", trade.TradeID, trade.Pair, trade.Price, trade.Quantity)
			}
		} else {
			hlog.Infof("订��未成交，推送订单簿变更, order_id=%s, pair=%s", order.OrderID, order.Pair)
			// 未成交，推送订单簿�����更
			depth := orderBook.DepthSnapshot()
			bids, _ = depth["buys"]
			asks, _ = depth["sells"]
			version := time.Now().UnixMilli() // 用时间戳做快照版本
			messageID := fmt.Sprintf("%s-%d-%d", order.OrderID, version, time.Now().UnixNano()%10000)
			traceID := fmt.Sprintf("%s-%d", order.OrderID, version)
			result := map[string]interface{}{
				"channel": pair,
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
				Broadcast(pair, msg)
			}

			// 增量快照推送
			delta := orderBook.DeltaSnapshot(lastSnapshot)
			if len(delta["bids_delta"].(map[string]string)) > 0 || len(delta["asks_delta"].(map[string]string)) > 0 {
				deltaResult := map[string]interface{}{
					"channel": pair,
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
					Broadcast(pair, deltaMsg)
				}
			}
			// ���新 lastSnapshot
			// 重新生成全量 bids/asks map
			newBids := map[string]string{}
			for _, b := range bids.([]map[string]string) {
				newBids[b["price"]] = b["quantity"]
			}
			newAsks := map[string]string{}
			for _, a := range asks.([]map[string]string) {
				newAsks[a["price"]] = a["quantity"]
			}
			lastSnapshot = &OrderBookSnapshot{Bids: newBids, Asks: newAsks}
		}
		// 缓存订单簿快照到 Redis
		cacheOrderBookSnapshot(pair, bids, asks)
	}
}

// Trade 简单订单簿实现（仅示例，实际可用更高效结构）
// Trade 回报结构体，支持多档撮合
type Trade struct {
	Pair       string `json:"pair"`
	Price      string `json:"price"`
	Quantity   string `json:"quantity"`
	Timestamp  int64  `json:"timestamp"`
	TakerOrder string `json:"taker_order_id"`
	MakerOrder string `json:"maker_order_id"`
	Side       string `json:"side"`
	EngineID   string `json:"engine_id"`
	TradeID    string `json:"trade_id"`
	TakerUser  string `json:"taker_user"`
	MakerUser  string `json:"maker_user"`
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

func Broadcast(channel string, msg []byte) {
	server.Broadcast(channel, msg)
}

var (
	kafkaWriter    *kafka.Writer
	tradeBatchChan chan Trade
)

func InitKafkaWriter(brokers []string, topic string) {
	kafkaWriter = &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		Async: true,
	}
	tradeBatchChan = make(chan Trade, 10000)
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

func saveTradeToKafka(trade Trade) {
	if tradeBatchChan != nil {
		tradeBatchChan <- trade
	}
}

var pgPool *pgxpool.Pool

func InitPostgresPool(connStr string) error {
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return err
	}
	pgPool = pool
	return nil
}

func saveTradeToDB(trade Trade) {
	if pgPool == nil {
		hlog.Warnf("Postgres连接池未初始化，无法持久化成交, trade_id=%s", trade.TradeID)
		return
	}
	_, err := pgPool.Exec(context.Background(),
		`INSERT INTO trades (trade_id, pair, price, quantity, timestamp, taker_order_id, maker_order_id, side, engine_id, taker_user, maker_user)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		trade.TradeID, trade.Pair, trade.Price, trade.Quantity, trade.Timestamp, trade.TakerOrder, trade.MakerOrder, trade.Side, trade.EngineID, trade.TakerUser, trade.MakerUser,
	)
	if err != nil {
		hlog.Errorf("持久化成交到Postgres失败, trade_id=%s, err=%v", trade.TradeID, err)
	}
}

var redisClient *redis.Client

func InitRedis(addr, password string, db int) {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}

// 缓存订单簿快照到 Redis
func cacheOrderBookSnapshot(pair string, bids, asks interface{}) {
	if redisClient == nil {
		return
	}
	ctx := context.Background()
	key := "orderbook:" + pair
	val, err := json.Marshal(map[string]interface{}{"bids": bids, "asks": asks})
	if err == nil {
		redisClient.Set(ctx, key, val, 5*time.Second)
	}
}

// 缓存成交记录到 Redis List
func cacheTrade(pair string, trade Trade, maxLen int64) {
	if redisClient == nil {
		return
	}
	ctx := context.Background()
	key := "trades:" + pair
	val, err := json.Marshal(trade)
	if err == nil {
		redisClient.LPush(ctx, key, val)
		redisClient.LTrim(ctx, key, 0, maxLen-1)
	}
}

// 缓存用户活跃订单ID到 Redis
func cacheUserActiveOrder(userID, orderID string) {
	if redisClient == nil || userID == "" || orderID == "" {
		return
	}
	ctx := context.Background()
	key := "user:active_orders:" + userID
	redisClient.SAdd(ctx, key, orderID)
	redisClient.Expire(ctx, key, 24*time.Hour)
}

// 从 Redis 移除用户活跃订单ID
func removeUserActiveOrder(userID, orderID string) {
	if redisClient == nil || userID == "" || orderID == "" {
		return
	}
	ctx := context.Background()
	key := "user:active_orders:" + userID
	redisClient.SRem(ctx, key, orderID)
}

// 查询用户活跃订单ID列表
func GetUserActiveOrders(userID string) ([]string, error) {
	if redisClient == nil || userID == "" {
		return nil, nil
	}
	ctx := context.Background()
	key := "user:active_orders:" + userID
	return redisClient.SMembers(ctx, key).Result()
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
// 可扩展更多字段
// 用于撮合引擎内部流转
type EngineOrderMsg struct {
	OrderID   string      `json:"order_id"`
	Pair      string      `json:"pair"`
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
func IsLocalMatchEngine(symbol string) bool {
	if consulHelper == nil {
		return true // 未启用Consul则全部本地处理
	}
	// 假设本节点注册时的pairs在内存中
	// 这里简单用环境变量/全局变量判断
	// 实际可维护一份本节点负责的symbol集合
	return isLocalSymbol(symbol)
}

// isLocalSymbol 判断symbol是否属于本节点
func isLocalSymbol(symbol string) bool {
	// 可用全局变量或配置保存本节点负责的pairs
	// 这里用环境变量CEX_MATCH_PAIRS
	pairs := getLocalPairs(cfg)
	for _, p := range pairs {
		if p == symbol {
			return true
		}
	}
	return false
}

// getLocalPairs 通过传入配置获取本节点负责的交易对
func getLocalPairs(cfg *Config) []string {
	return cfg.MatchPairs
}

// ParsePairs 工具函数，解析逗号分隔的交易对字符串
func ParsePairs(s string) []string {
	var res []string
	for _, p := range splitAndTrim(s, ",") {
		if p != "" {
			res = append(res, p)
		}
	}
	return res
}

func splitAndTrim(s, sep string) []string {
	var res []string
	for _, v := range split(s, sep) {
		res = append(res, trim(v))
	}
	return res
}

func split(s, sep string) []string {
	var res []string
	for _, v := range bytes.Split([]byte(s), []byte(sep)) {
		res = append(res, string(v))
	}
	return res
}

func trim(s string) string {
	return string(bytes.TrimSpace([]byte(s)))
}

// ForwardOrderToMatchEngine 转发订单到目标撮合节点（HTTP示例）
func ForwardOrderToMatchEngine(symbol string, data []byte) error {
	if consulHelper == nil {
		return fmt.Errorf("consul not initialized")
	}
	nodes, err := consulHelper.DiscoverMatchEngine(symbol)
	if err != nil || len(nodes) == 0 {
		return fmt.Errorf("no match engine found for symbol %s", symbol)
	}
	// 取第一个节点（可做负载均衡）
	url := fmt.Sprintf("http://%s:%d/submit_order", nodes[0].Address, nodes[0].Port)
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
