package handler

import (
	"cex-hertz/biz/dal/pg"
	"cex-hertz/biz/model"
	"context"
	"fmt"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

type SubmitOrderRequest struct {
	OrderID  string `json:"order_id"`
	Pair     string `json:"pair"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

type SubmitOrderResponse struct {
	Type    string `json:"type"`
	OrderID string `json:"order_id"`
	Pair    string `json:"pair"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// SubmitOrder RESTful 下单接口
func SubmitOrder(ctx context.Context, c *app.RequestContext) {
	var req model.Order
	if err := c.BindAndValidate(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	if req.OrderID == "" || req.Pair == "" || req.Side == "" || req.Price == "" || req.Quantity == "" || req.UserID == "" {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "missing required fields"})
		return
	}
	req.Status = "active"
	req.UpdatedAt = req.CreatedAt
	if err := pg.CreateOrder(&req); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, map[string]interface{}{"order_id": req.OrderID, "status": "received"})
}

// GetOrder 查询单个订单
func GetOrder(ctx context.Context, c *app.RequestContext) {
	orderID := c.Param("id")
	order, err := pg.GetOrder(orderID)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]interface{}{"error": "order not found"})
		return
	}
	c.JSON(consts.StatusOK, order)
}

// ListOrders 查询订单列表
func ListOrders(ctx context.Context, c *app.RequestContext) {
	userID := string(c.Query("user_id"))
	status := string(c.Query("status"))
	orders, err := pg.ListOrders(userID, status)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, orders)
}

// 取消订单
func CancelOrder(ctx context.Context, c *app.RequestContext) {
	type CancelOrderRequest struct {
		OrderID string `json:"order_id"`
		UserID  string `json:"user_id"`
	}
	var req CancelOrderRequest
	if err := c.BindAndValidate(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "invalid request"})
		return
	}
	order, err := pg.GetOrder(req.OrderID)
	if err != nil || order.UserID != req.UserID {
		c.JSON(consts.StatusNotFound, map[string]interface{}{"error": "order not found or user mismatch"})
		return
	}
	if err := pg.UpdateOrderStatus(req.OrderID, "cancelled"); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, map[string]interface{}{"order_id": req.OrderID, "status": "cancelled"})
}

// 查询成交记录（GORM）
func ListTrades(ctx context.Context, c *app.RequestContext) {
	pair := string(c.Query("pair"))
	limit := 50
	if l := c.Query("limit"); len(l) > 0 {
		fmt.Sscanf(string(l), "%d", &limit)
	}
	trades, err := pg.ListTrades(pair, limit)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, trades)
}
