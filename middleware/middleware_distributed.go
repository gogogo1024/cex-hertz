package middleware

import (
	"cex-hertz/biz/service"
	"cex-hertz/biz/util"
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// DistributedRouteMiddleware 是分布式撮合自动路由中间件
func DistributedRouteMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// 只拦截 /api/order 下单接口
		if string(c.Path()) == "/api/order" && string(c.Request.Method()) == consts.MethodPost {
			var req map[string]interface{}
			if err := c.BindAndValidate(&req); err != nil {
				c.String(400, "invalid request")
				c.Abort()
				return
			}
			pair, _ := req["pair"].(string)
			if pair == "" {
				c.String(400, "pair required")
				c.Abort()
				return
			}
			if !util.IsLocalMatchEngine(pair) {
				if err := service.ForwardOrderToMatchEngine(pair, c.Request.Body()); err != nil {
					hlog.Errorf("order forward failed: %v", err)
					c.String(502, "order forward failed: %v", err)
					c.Abort()
					return
				}
				c.String(200, "order forwarded")
				c.Abort()
				return
			}
		}
		c.Next(ctx)
	}
}
