package handler

import (
	"net/http"
	"os"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"

	"cex-hertz/biz/model"
)

var (
	partitionTableOnce sync.Once
	partitionTable     *model.PartitionTable
)

// GetPartitionTable 获取全局分区表（可替换为实际注入或持久化加载）
func GetPartitionTable() *model.PartitionTable {
	partitionTableOnce.Do(func() {
		// TODO: 实际项目应从持久化存储或服务加载分区表
		partitionTable = model.NewPartitionTable()
		// 示例：初始化 symbol 映射到多个分区
		partitionTable.SymbolToPartition["BTCUSDT"] = []string{"partition-1", "partition-2"}
		partitionTable.SymbolToPartition["ETHUSDT"] = []string{"partition-1"}
	})
	return partitionTable
}

// GetCurrentPartitionID 获取当前分区ID（可用 NodeID 作为分区ID）
func GetCurrentPartitionID() string {
	return os.Getenv("CEX_NODE_ID")
}

// SymbolMigrationChecker 判断 symbol 是否已迁移到新分区
func SymbolMigrationChecker(symbol string) (migrated bool, newPartitionID string) {
	table := GetPartitionTable()
	currentID := GetCurrentPartitionID()
	partitionIDs := table.SymbolToPartition[symbol]
	if len(partitionIDs) > 1 {
		// 当前分区不是主分区（第一个），则认为 symbol 已迁移
		if partitionIDs[0] != currentID {
			return true, partitionIDs[0]
		}
	}
	return false, ""
}

// MigrateRedirectHandler 旧分区收到 symbol 的写请求时，判断是否迁移，若迁移则重定向
func MigrateRedirectHandler(ctx *app.RequestContext) {
	symbol := string(ctx.Query("symbol"))
	migrated, newPartitionID := SymbolMigrationChecker(symbol)
	if migrated {
		ctx.JSON(http.StatusConflict, map[string]interface{}{
			"code":             40901,
			"msg":              "symbol已迁移，请重试到新分区",
			"new_partition_id": newPartitionID,
		})
		return
	}
	// ...正常处理写请求逻辑...
	ctx.JSON(http.StatusOK, map[string]interface{}{
		"code": 0,
		"msg":  "写入成功",
	})
}
