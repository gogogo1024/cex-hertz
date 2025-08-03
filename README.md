# cex-hertz 项目结构与模块说明

本项目为高性能中心化交易所（CEX）撮合系统，基于 Hertz 框架开发，支持 HTTP 和 WebSocket 接口，具备分布式撮合、订单管理、行情推送等功能。

## 目录结构与主要模块

```
biz/
  dal/
    kafka/         # Kafka初始化与写入
    pg/            # Postgres数据库相关
    redis/         # Redis缓存相关
  engine/          # 撮合引擎接口
  handler/         # HTTP与WebSocket接口处理
  model/           # 业务数据结构定义（订单、撮合结果等）
  service/         # 核心业务逻辑（撮合引擎、订单簿、Consul注册等）
  router/          # 路由注册
  util/            # 工具函数
cmd/               # 命令行入口
conf/              # 配置文件与加载逻辑
  dev/             # 开发环境配置
  test/            # 测试环境配置
server/            # WebSocket服务端实现、频道管理
handler/           # WebSocket相关处理
middleware/        # 分布式撮合中间件
pkg/               # 公共包
script/            # 启动脚本
benchmark/         # 性能测试脚本
cex-fe/            # 前端项目（Vite+React）
doc/               # 设计文档
main.go            # 启动入口
router.go          # 路由注册
Dockerfile         # Docker构建文件
docker-compose-base.yaml # 一键启动依赖服务
```

## 主要模块说明

- **biz/dal/**
  - `kafka/`：Kafka初始化与批量写入
  - `pg/`：Postgres数据库访问，订单/成交批量插入
  - `redis/`：Redis缓存初始化
- **biz/engine/**
  - `engine.go`：撮合引擎接口定义
- **biz/handler/**
  - `order.go`：HTTP下单、订单相关接口
  - `ws_handler.go`：WebSocket消息处理
  - `ping.go`：健康检查接口
- **biz/model/**
  - `order.go`、`trade.go`、`kline.go`：核心数据结构
- **biz/service/**
  - `match_engine.go`：撮合引擎核心逻辑，支持批量订单消费与原生多值INSERT批量入库
  - `orderbook.go/orderbook_manager.go`：订单簿管理
  - `consul_helper.go`：Consul注册与服务发现
- **server/**
  - `websocket_server.go`：WebSocket服务端实现、频道订阅/广播
- **conf/**
  - `conf.go`：配置加载逻辑
  - `dev/`、`test/`：不同环境的配置文件
- **util/**
  - `util.go`：通用工具函数
- **middleware/**
  - `distributed.go`：分布式撮合路由中间件
- **benchmark/**
  - `tsung.xml`：压力测试脚本
- **cex-fe/**
  - 前端项目，基于Vite+React
- **doc/**
  - 设计文档、系统架构说明

## 新特性说明

- 撮合引擎支持 Kafka 批量消费，积压消息可高效批量处理。
- 订单批量入库采用原生多值 INSERT 语法，显著提升 Postgres 性能。
- 支持分布式撮合、Consul 服务发现、WebSocket 实时推送。
- 前后端分离，前端项目位于 cex-fe/，支持行情展示与下单。

## 启动与测试

1. 启动依赖服务：
   ```sh
   docker-compose -f docker-compose-base.yaml up -d
   ```
2. 启动后端服务：
   ```sh
   go run main.go
   ```
3. 启动前端服务：
   ```sh
   cd cex-fe && npm install && npm run dev
   ```
4. 压测可参考 benchmark/tsung.xml。

---
如需详细文档请参考 doc/ 目录。
