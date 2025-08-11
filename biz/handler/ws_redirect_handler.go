package handler

// MigrateWSRedirectHandler（WebSocket场景）
// 用于 symbol 迁移期间，WebSocket 写请求（如 SubmitOrder）重定向到新分区，直接向连接发送重定向消息
func MigrateWSRedirectHandler(conn interface{ WriteMessage(int, []byte) error }, mt int, symbol string, newPartitionID string) {
	msg := []byte(`{"type":"migrate_redirect","symbol":"` + symbol + `","new_partition_id":"` + newPartitionID + `"}`)
	_ = conn.WriteMessage(mt, msg)
}
