package service

import (
	"context"
	"fmt"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/redis"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"

	"github.com/huandu/skiplist"
)

type OrderBook struct {
	symbol string
	buys   *skiplist.SkipList // 买单：价格降序
	sells  *skiplist.SkipList // 卖单：价格升序
}

func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		symbol: symbol,
		buys:   skiplist.New(priceComparator{}),
		sells:  skiplist.New(priceAscComparator{}),
	}
}

func (ob *OrderBook) Match(order model.SubmitOrderMsg) ([]model.Trade, bool) {
	var trades []model.Trade
	remainQty := toFloat(order.Quantity)

	switch order.Side {
	case "buy":
		trades, remainQty = ob.matchBuyOrder(order, remainQty)
		if remainQty > 0 {
			ob.addOrder(ob.buys, order)
		}
		return trades, len(trades) > 0
	case "sell":
		trades, remainQty = ob.matchSellOrder(order, remainQty)
		if remainQty > 0 {
			ob.addOrder(ob.sells, order)
		}
		return trades, len(trades) > 0
	default:
		return trades, false
	}
}

func (ob *OrderBook) matchBuyOrder(order model.SubmitOrderMsg, remainQty float64) ([]model.Trade, float64) {
	var trades []model.Trade
	for remainQty > 0 && ob.sells.Len() > 0 {
		minSellElem := ob.sells.Front()
		minSellPrice := minSellElem.Key().(string)
		if toFloat(order.Price) < toFloat(minSellPrice) {
			break
		}
		sellQueue := minSellElem.Value.([]model.SubmitOrderMsg)
		sell := &sellQueue[0]
		makerQty := toFloat(sell.Quantity)
		tradeQty := minFloat(remainQty, makerQty)
		trades = append(trades, model.Trade{
			Price:        sell.Price,
			Quantity:     fmt.Sprintf("%.8f", tradeQty),
			TakerOrderID: order.OrderID,
			MakerOrderID: sell.OrderID,
			Side:         "buy",
		})
		remainQty -= tradeQty
		sell.Quantity = fmt.Sprintf("%.8f", makerQty-tradeQty)
		if sell.Quantity == "0.00000000" {
			sellQueue = sellQueue[1:]
			if len(sellQueue) == 0 {
				ob.sells.Remove(minSellElem)
			} else {
				minSellElem.Value = sellQueue
			}
		} else {
			minSellElem.Value = sellQueue
		}
	}
	return trades, remainQty
}

func (ob *OrderBook) matchSellOrder(order model.SubmitOrderMsg, remainQty float64) ([]model.Trade, float64) {
	var trades []model.Trade
	for remainQty > 0 && ob.buys.Len() > 0 {
		maxBuyElem := ob.buys.Front()
		maxBuyPrice := maxBuyElem.Key().(string)
		if toFloat(order.Price) > toFloat(maxBuyPrice) {
			break
		}
		buyQueue := maxBuyElem.Value.([]model.SubmitOrderMsg)
		buy := &buyQueue[0]
		makerQty := toFloat(buy.Quantity)
		tradeQty := minFloat(remainQty, makerQty)
		trades = append(trades, model.Trade{
			Price:        buy.Price,
			Quantity:     fmt.Sprintf("%.8f", tradeQty),
			TakerOrderID: order.OrderID,
			MakerOrderID: buy.OrderID,
			Side:         "sell",
		})
		remainQty -= tradeQty
		buy.Quantity = fmt.Sprintf("%.8f", makerQty-tradeQty)
		if buy.Quantity == "0.00000000" {
			buyQueue = buyQueue[1:]
			if len(buyQueue) == 0 {
				ob.buys.Remove(maxBuyElem)
			} else {
				maxBuyElem.Value = buyQueue
			}
		} else {
			maxBuyElem.Value = buyQueue
		}
	}
	return trades, remainQty
}

func (ob *OrderBook) addOrder(book *skiplist.SkipList, order model.SubmitOrderMsg) {
	if elem := book.Get(order.Price); elem != nil {
		queue := elem.Value.([]model.SubmitOrderMsg)
		queue = append(queue, order)
		elem.Value = queue
	} else {
		book.Set(order.Price, []model.SubmitOrderMsg{order})
	}
}

func (ob *OrderBook) DepthSnapshot() map[string]interface{} {
	var bids []map[string]string
	for iter := ob.buys.Front(); iter != nil; iter = iter.Next() {
		queue := iter.Value.([]model.SubmitOrderMsg)
		for _, o := range queue {
			bids = append(bids, map[string]string{"price": o.Price, "quantity": o.Quantity})
		}
	}
	var asks []map[string]string
	for iter := ob.sells.Front(); iter != nil; iter = iter.Next() {
		queue := iter.Value.([]model.SubmitOrderMsg)
		for _, o := range queue {
			asks = append(asks, map[string]string{"price": o.Price, "quantity": o.Quantity})
		}
	}
	return map[string]interface{}{
		"buys":  bids,
		"sells": asks,
	}
}

type OrderBookSnapshot struct {
	Bids map[string]string // price -> quantity
	Asks map[string]string // price -> quantity
}

func (ob *OrderBook) DeltaSnapshot(last *OrderBookSnapshot) map[string]interface{} {
	currBids := ob.getCurrentSnapshot(ob.buys)
	currAsks := ob.getCurrentSnapshot(ob.sells)
	bidsDelta := calculateDelta(currBids, last, true)
	asksDelta := calculateDelta(currAsks, last, false)
	return map[string]interface{}{
		"bids_delta": bidsDelta,
		"asks_delta": asksDelta,
	}
}

func (ob *OrderBook) getCurrentSnapshot(book *skiplist.SkipList) map[string]string {
	result := map[string]string{}
	for iter := book.Front(); iter != nil; iter = iter.Next() {
		queue := iter.Value.([]model.SubmitOrderMsg)
		for _, o := range queue {
			result[o.Price] = o.Quantity
		}
	}
	return result
}

func calculateDelta(curr map[string]string, last *OrderBookSnapshot, isBid bool) map[string]string {
	delta := map[string]string{}
	var lastMap map[string]string
	if last != nil {
		if isBid {
			lastMap = last.Bids
		} else {
			lastMap = last.Asks
		}
	}
	for price, qty := range curr {
		if lastMap == nil || lastMap[price] != qty {
			delta[price] = qty
		}
	}
	for price := range lastMap {
		if _, ok := curr[price]; !ok {
			delta[price] = "0"
		}
	}
	return delta
}

// CancelOrder 撤单功能：根据 order_id 和 side 撤销订单
func (ob *OrderBook) CancelOrder(orderID, side string, userID string) bool {
	var book *skiplist.SkipList
	switch side {
	case "buy":
		book = ob.buys
	case "sell":
		book = ob.sells
	default:
		return false
	}
	return cancelOrderInBook(book, orderID, userID)
}

func cancelOrderInBook(book *skiplist.SkipList, orderID, userID string) bool {
	for iter := book.Front(); iter != nil; iter = iter.Next() {
		queue := iter.Value.([]model.SubmitOrderMsg)
		idx := findOrderIndex(queue, orderID)
		if idx != -1 {
			queue = append(queue[:idx], queue[idx+1:]...)
			if len(queue) == 0 {
				book.Remove(iter)
			} else {
				iter.Value = queue
			}
			if userID != "" {
				removeUserActiveOrder(userID, orderID)
			}
			return true
		}
	}
	return false
}

func findOrderIndex(queue []model.SubmitOrderMsg, orderID string) int {
	for i, o := range queue {
		if o.OrderID == orderID {
			return i
		}
	}
	return -1
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

// 跳表价格比较器
// 实现 skiplist.Comparable 接口
type priceComparator struct{}

func (priceComparator) Compare(l, r interface{}) int {
	lf, rf := toFloat(l.(string)), toFloat(r.(string))
	if lf > rf {
		return -1 // 买单：价格高优先
	} else if lf < rf {
		return 1
	}
	return 0
}
func (priceComparator) CalcScore(key interface{}) float64 {
	return toFloat(key.(string))
}

type priceAscComparator struct{}

func (priceAscComparator) Compare(l, r interface{}) int {
	lf, rf := toFloat(l.(string)), toFloat(r.(string))
	if lf < rf {
		return -1 // 卖单：价格低优先
	} else if lf > rf {
		return 1
	}
	return 0
}
func (priceAscComparator) CalcScore(key interface{}) float64 {
	return toFloat(key.(string))
}
