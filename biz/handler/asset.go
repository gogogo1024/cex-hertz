package handler

import (
	"cex-hertz/biz/service"
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// GetBalance 查询用户余额
func GetBalance(ctx context.Context, c *app.RequestContext) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "missing user_id"})
		return
	}
	balances, err := service.GetUserBalance(userID)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, balances)
}

// GetPositions 查询用户持仓
func GetPositions(ctx context.Context, c *app.RequestContext) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "missing user_id"})
		return
	}
	positions, err := service.GetUserPositions(userID)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, positions)
}
