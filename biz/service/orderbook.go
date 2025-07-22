package service

import (
	"cex-hertz/biz/model"
	"fmt"
	"github.com/huandu/skiplist"
)

type OrderBook struct {
	pair  string
	buys  *skiplist.SkipList // 买单：价格降序
	sells *skiplist.SkipList // 卖单：价格升序
}

func NewOrderBook(pair string) *OrderBook {
	return &OrderBook{
		pair:  pair,
		buys:  skiplist.New(priceComparator{}),
		sells: skiplist.New(priceAscComparator{}),
	}
}

func (ob *OrderBook) Match(order model.SubmitOrderMsg) ([]model.Trade, bool) {
	var trades []model.Trade
	remainQty := toFloat(order.Quantity)
	if order.Side == "buy" {
		for remainQty > 0 && ob.sells.Len() > 0 {
			minSellElem := ob.sells.Front()
			minSellPrice := minSellElem.Key().(string)
			if toFloat(order.Price) >= toFloat(minSellPrice) {
				sellQueue := minSellElem.Value.([]model.SubmitOrderMsg)
				sell := &sellQueue[0]
				makerQty := toFloat(sell.Quantity)
				tradeQty := minFloat(remainQty, makerQty)
				trades = append(trades, model.Trade{
					Price:      sell.Price,
					Quantity:   fmt.Sprintf("%.8f", tradeQty),
					TakerOrder: order.OrderID,
					MakerOrder: sell.OrderID,
					Side:       "buy",
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
			} else {
				break
			}
		}
		if remainQty > 0 {
			ob.addOrder(ob.buys, order)
		}
		return trades, len(trades) > 0
	}
	if order.Side == "sell" {
		for remainQty > 0 && ob.buys.Len() > 0 {
			maxBuyElem := ob.buys.Front()
			maxBuyPrice := maxBuyElem.Key().(string)
			if toFloat(order.Price) <= toFloat(maxBuyPrice) {
				buyQueue := maxBuyElem.Value.([]model.SubmitOrderMsg)
				buy := &buyQueue[0]
				makerQty := toFloat(buy.Quantity)
				tradeQty := minFloat(remainQty, makerQty)
				trades = append(trades, model.Trade{
					Price:      buy.Price,
					Quantity:   fmt.Sprintf("%.8f", tradeQty),
					TakerOrder: order.OrderID,
					MakerOrder: buy.OrderID,
					Side:       "sell",
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
			} else {
				break
			}
		}
		if remainQty > 0 {
			ob.addOrder(ob.sells, order)
		}
		return trades, len(trades) > 0
	}
	return trades, false
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
	currBids := map[string]string{}
	for iter := ob.buys.Front(); iter != nil; iter = iter.Next() {
		queue := iter.Value.([]model.SubmitOrderMsg)
		for _, o := range queue {
			currBids[o.Price] = o.Quantity
		}
	}
	currAsks := map[string]string{}
	for iter := ob.sells.Front(); iter != nil; iter = iter.Next() {
		queue := iter.Value.([]model.SubmitOrderMsg)
		for _, o := range queue {
			currAsks[o.Price] = o.Quantity
		}
	}
	bidsDelta := map[string]string{}
	asksDelta := map[string]string{}
	for price, qty := range currBids {
		if last == nil || last.Bids == nil || last.Bids[price] != qty {
			bidsDelta[price] = qty
		}
	}
	if last != nil && last.Bids != nil {
		for price := range last.Bids {
			if _, ok := currBids[price]; !ok {
				bidsDelta[price] = "0"
			}
		}
	}
	for price, qty := range currAsks {
		if last == nil || last.Asks == nil || last.Asks[price] != qty {
			asksDelta[price] = qty
		}
	}
	if last != nil && last.Asks != nil {
		for price := range last.Asks {
			if _, ok := currAsks[price]; !ok {
				asksDelta[price] = "0"
			}
		}
	}
	return map[string]interface{}{
		"bids_delta": bidsDelta,
		"asks_delta": asksDelta,
	}
}

// CancelOrder 撤单功能：根据 order_id 和 side 撤销订单
func (ob *OrderBook) CancelOrder(orderID, side string, userID string) bool {
	var book *skiplist.SkipList
	if side == "buy" {
		book = ob.buys
	} else if side == "sell" {
		book = ob.sells
	} else {
		return false
	}
	for iter := book.Front(); iter != nil; iter = iter.Next() {
		queue := iter.Value.([]model.SubmitOrderMsg)
		for i, o := range queue {
			if o.OrderID == orderID {
				queue = append(queue[:i], queue[i+1:]...)
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
	}
	return false
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
