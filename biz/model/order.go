package model

// SubmitOrderMsg 订单结构体（与 handler 保持一致）
type SubmitOrderMsg struct {
	OrderID  string
	Pair     string
	Side     string
	Price    string
	Quantity string
	UserID   string // 新增 UserID 字段
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
