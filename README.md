# cex-hertz 项目结构与模块说明

本项目为高性能中心化交易所（CEX）撮合系统，基于 Hertz 框架开发，支持 HTTP 和 WebSocket 接口，具备分布式撮合、订单管理、行情推送等功能。

## 目录结构与主要模块

```
biz/
  handler/    # 业务接口处理（HTTP、WebSocket）
  model/      # 业务数据结构定义（如订单、撮合结果）
  service/    # 核心业务逻辑（撮合引擎、订单簿、Consul注册等）
  dal/        # 数据访问层（数据库、Redis、Kafka 初始化等）
  util/       # 工具函数
conf/         # 配置文件与加载逻辑
server/       # WebSocket 服务端实现、频道管理
handler/      # WebSocket 相关处理（如 ws_handler.go）
main.go       # 启动入口
router.go     # 路由注册
Dockerfile    # Docker 构建文件
docker-compose-base.yaml # 一键启动依赖服务
```

## 主要模块说明

- **biz/handler/**
  - `order.go`：HTTP 下单、订单相关接口
  - `ws_handler.go`：WebSocket 消息处理（如订阅、下单、撮合结果推送）
  - `middleware_distributed.go`：分布式撮合路由中间件
  - `ping.go`：健康检查接口

- **biz/model/**
  - `order.go`：订单、撮合等核心数据结构定义

- **biz/service/**
  - `match_engine.go`：撮合引擎核心逻辑
  - `orderbook.go/orderbook_manager.go`：订单簿管理
  - `consul_helper.go`：Consul 注册与服务发现

- **biz/dal/**
  - `pg/`、`redis/`、`kafka/`：数据库、缓存、消息队列初始化

- **server/**
  - `websocket_server.go`：WebSocket 服务端实现、频道订阅/广播

- **conf/**
  - `conf.go`：配置加载逻辑
  - `dev/`、`test/`：不同环境的配置文件

- **util/**
  - `util.go`：通用工具函数

- **main.go**
  - 项目启动入口，初始化各模块、注册路由、中间件、启动 HTTP/WS 服务

- **router.go**
  - 路由注册（自动生成）

- **docker-compose-base.yaml**
  - 一键启动 Consul、Postgres、Redis、Kafka 等依赖服务

---

如需详细接口文档或模块用法说明，请查阅各目录下 README 或源码注释。

