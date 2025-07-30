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
		hlog.Infof("[DistributedRouteMiddleware] path=%s, method=%s", c.Path(), c.Request.Method())
		// 只拦截 /api/order 下单接口
		if string(c.Path()) == "/api/order" && string(c.Request.Method()) == consts.MethodPost {
			var req map[string]interface{}
			if err := c.BindAndValidate(&req); err != nil {
				hlog.Errorf("[DistributedRouteMiddleware] invalid request: %v", err)
				c.String(400, "invalid request")
				c.Abort()
				return
			}
			symbol, _ := req["symbol"].(string)
			if symbol == "" {
				hlog.Errorf("[DistributedRouteMiddleware] symbol required")
				c.String(400, "symbol required")
				c.Abort()
				return
			}
			if !util.IsLocalMatchEngine(symbol) {
				hlog.Infof("[DistributedRouteMiddleware] forward order for symbol=%s", symbol)
				if err := service.ForwardOrderToMatchEngine(symbol, c.Request.Body()); err != nil {
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
		hlog.Infof("[DistributedRouteMiddleware] pass through, path=%s", c.Path())
		c.Next(ctx)
	}
}
