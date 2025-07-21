package service

import "sync"

// 分片OrderBook管理器
// 按symbol分片，每个symbol一个OrderBook

type OrderBookManager struct {
	books map[string]*OrderBook
	mu    sync.RWMutex
}

// NewOrderBookManager 新建OrderBookManager
func NewOrderBookManager() *OrderBookManager {
	return &OrderBookManager{
		books: make(map[string]*OrderBook),
	}
}

// GetOrderBook 获取指定symbol的OrderBook，不存在则新建
func (m *OrderBookManager) GetOrderBook(symbol string) *OrderBook {
	m.mu.RLock()
	ob, ok := m.books[symbol]
	m.mu.RUnlock()
	if ok {
		return ob
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// double check
	ob, ok = m.books[symbol]
	if ok {
		return ob
	}
	ob = NewOrderBook(symbol)
	m.books[symbol] = ob
	return ob
}

// CancelOrder 分片撤单接口
func (m *OrderBookManager) CancelOrder(symbol, orderID, side, userID string) bool {
	ob := m.GetOrderBook(symbol)
	return ob.CancelOrder(orderID, side, userID)
}
