package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"cex-hertz/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
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
}

var Engine = NewMatchEngine()

func NewMatchEngine() *MatchEngine {
	return &MatchEngine{
		orderQueues: make(map[string]chan SubmitOrderMsg),
	}
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
	queue <- order
}

// 每个交易对独立撮合 worker
func (m *MatchEngine) matchWorker(pair string, queue chan SubmitOrderMsg) {
	orderBook := NewOrderBook(pair)
	engineID := "node-1" // 可通过配置或环境变量设置
	hlog.Infof("撮合线程启动, pair=%s, engine_id=%s", pair, engineID)
	for order := range queue {
		hlog.Infof("撮合订单, order_id=%s, pair=%s, side=%s, price=%s, quantity=%s", order.OrderID, order.Pair, order.Side, order.Price, order.Quantity)
		trades, filled := orderBook.Match(order)
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
				hlog.Infof("成交回报, trade_id=%s, pair=%s, price=%s, quantity=%s", trade.TradeID, trade.Pair, trade.Price, trade.Quantity)
			}
		} else {
			hlog.Infof("订单未成交，推送订单簿变更, order_id=%s, pair=%s", order.OrderID, order.Pair)
			// 未成交，推送订单簿变更
			depth := orderBook.DepthSnapshot()
			// 正确从 map 取出 bids、asks、version 字段
			bids, _ := depth["buys"]
			asks, _ := depth["sells"]
			version, _ := depth["version"]
			messageID := fmt.Sprintf("%s-%d-%d", order.OrderID, time.Now().UnixMilli(), time.Now().UnixNano()%10000)
			traceID := fmt.Sprintf("%s-%d", order.OrderID, time.Now().UnixMilli())
			result := map[string]interface{}{
				"channel": pair,
				"type":    "depth_update",
				"data": map[string]interface{}{
					"bids":      bids,
					"asks":      asks,
					"timestamp": time.Now().UnixMilli(),
					"version":   version,
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
		}
	}
}

// 简单订单簿实现（仅示例，实际可用更高效结构）
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

type OrderBook struct {
	pair  string
	buys  []SubmitOrderMsg // 买单按价格降序、时间优先
	sells []SubmitOrderMsg // 卖单按价格升序、时间优先
}

func NewOrderBook(pair string) *OrderBook {
	return &OrderBook{pair: pair}
}

// Match 多档撮合，返回所有成交回报和剩余未成交
func (ob *OrderBook) Match(order SubmitOrderMsg) ([]Trade, bool) {
	trades := []Trade{}
	remainQty := toFloat(order.Quantity)
	if order.Side == "buy" {
		ob.sortSells()
		for i := 0; i < len(ob.sells) && remainQty > 0; {
			sell := ob.sells[i]
			if toFloat(order.Price) >= toFloat(sell.Price) {
				makerQty := toFloat(sell.Quantity)
				tradeQty := minFloat(remainQty, makerQty)
				trades = append(trades, Trade{
					Price:      sell.Price,
					Quantity:   fmt.Sprintf("%.8f", tradeQty),
					TakerOrder: order.OrderID,
					MakerOrder: sell.OrderID,
					Side:       "buy",
				})
				remainQty -= tradeQty
				ob.sells[i].Quantity = fmt.Sprintf("%.8f", makerQty-tradeQty)
				if ob.sells[i].Quantity == "0.00000000" {
					ob.sells = append(ob.sells[:i], ob.sells[i+1:]...)
				} else {
					i++
				}
			} else {
				break
			}
		}
		if remainQty > 0 {
			order.Quantity = fmt.Sprintf("%.8f", remainQty)
			ob.buys = append(ob.buys, order)
			ob.sortBuys()
		}
		return trades, len(trades) > 0
	}
	if order.Side == "sell" {
		ob.sortBuys()
		for i := 0; i < len(ob.buys) && remainQty > 0; {
			buy := ob.buys[i]
			if toFloat(order.Price) <= toFloat(buy.Price) {
				makerQty := toFloat(buy.Quantity)
				tradeQty := minFloat(remainQty, makerQty)
				trades = append(trades, Trade{
					Price:      buy.Price,
					Quantity:   fmt.Sprintf("%.8f", tradeQty),
					TakerOrder: order.OrderID,
					MakerOrder: buy.OrderID,
					Side:       "sell",
				})
				remainQty -= tradeQty
				ob.buys[i].Quantity = fmt.Sprintf("%.8f", makerQty-tradeQty)
				if ob.buys[i].Quantity == "0.00000000" {
					ob.buys = append(ob.buys[:i], ob.buys[i+1:]...)
				} else {
					i++
				}
			} else {
				break
			}
		}
		if remainQty > 0 {
			order.Quantity = fmt.Sprintf("%.8f", remainQty)
			ob.sells = append(ob.sells, order)
			ob.sortSells()
		}
		return trades, len(trades) > 0
	}
	return trades, false
}

// 买单按价格降序、时间优先
func (ob *OrderBook) sortBuys() {
	sort.SliceStable(ob.buys, func(i, j int) bool {
		if toFloat(ob.buys[i].Price) == toFloat(ob.buys[j].Price) {
			return i < j // 时间优先
		}
		return toFloat(ob.buys[i].Price) > toFloat(ob.buys[j].Price)
	})
}

// 卖单按价格升序、时间优先
func (ob *OrderBook) sortSells() {
	sort.SliceStable(ob.sells, func(i, j int) bool {
		if toFloat(ob.sells[i].Price) == toFloat(ob.sells[j].Price) {
			return i < j // 时间优先
		}
		return toFloat(ob.sells[i].Price) < toFloat(ob.sells[j].Price)
	})
}

// DepthSnapshot 返回当前订单簿快照（价格优先+时间优先）
func (ob *OrderBook) DepthSnapshot() map[string]interface{} {
	buys := make([]map[string]string, len(ob.buys))
	for i, o := range ob.buys {
		buys[i] = map[string]string{"price": o.Price, "quantity": o.Quantity}
	}
	sells := make([]map[string]string, len(ob.sells))
	for i, o := range ob.sells {
		sells[i] = map[string]string{"price": o.Price, "quantity": o.Quantity}
	}
	return map[string]interface{}{
		"buys":  buys,
		"sells": sells,
	}
}

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

var kafkaWriter *kafka.Writer

func InitKafkaWriter(brokers []string, topic string) {
	kafkaWriter = &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		Async: true,
	}
}

func saveTradeToKafka(trade Trade) {
	if kafkaWriter == nil {
		return
	}
	msgBytes, err := json.Marshal(trade)
	if err != nil {
		return
	}
	kafkaWriter.WriteMessages(
		context.Background(),
		kafka.Message{Value: msgBytes},
	)
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

// OrderStatus 订单状态常量
type OrderStatus string

const (
	OrderStatusSubmitted  OrderStatus = "submitted"
	OrderStatusPartFilled OrderStatus = "part_filled"
	OrderStatusFilled     OrderStatus = "filled"
	OrderStatusCancelled  OrderStatus = "cancelled"
	OrderStatusFailed     OrderStatus = "failed"
)

// EngineOrderMsg 内部订单结构体，包含状态、时间、用户等
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
