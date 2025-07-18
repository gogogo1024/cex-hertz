package handler

import (
	"cex-hertz/biz/service"
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
func SubmitOrder(ctx *app.RequestContext) {
	var req SubmitOrderRequest
	if err := ctx.BindAndValidate(&req); err != nil {
		ctx.JSON(consts.StatusBadRequest, SubmitOrderResponse{
			Type:    "order_ack",
			Status:  "error",
			Message: "invalid request: " + err.Error(),
		})
		return
	}
	if req.Pair == "" || req.Side == "" || req.Price == "" || req.Quantity == "" {
		ctx.JSON(consts.StatusBadRequest, SubmitOrderResponse{
			Type:    "order_ack",
			Status:  "error",
			Message: "missing required fields",
		})
		return
	}
	order := service.SubmitOrderMsg{
		OrderID:  req.OrderID,
		Pair:     req.Pair,
		Side:     req.Side,
		Price:    req.Price,
		Quantity: req.Quantity,
	}
	service.Engine.SubmitOrder(order)
	ctx.JSON(consts.StatusOK, SubmitOrderResponse{
		Type:    "order_ack",
		OrderID: req.OrderID,
		Pair:    req.Pair,
		Status:  "received",
	})
}
