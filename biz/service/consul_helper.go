package service

import (
	"fmt"
	"github.com/hashicorp/consul/api"
)

// ConsulHelper 封装 Consul 注册与发现
// 使用前请确保 Consul agent 已启动

type ConsulHelper struct {
	client *api.Client
}

// NewConsulHelper 创建 Consul 客户端
func NewConsulHelper(addr string) (*ConsulHelper, error) {
	cfg := api.DefaultConfig()
	cfg.Address = addr
	cli, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ConsulHelper{client: cli}, nil
}

// NewConsulHelperWithAddrs 支持多个 Consul 地址高可用
func NewConsulHelperWithAddrs(addrs []string) (*ConsulHelper, error) {
	var lastErr error
	for _, addr := range addrs {
		cfg := api.DefaultConfig()
		cfg.Address = addr
		cli, err := api.NewClient(cfg)
		if err == nil {
			// 尝试健康检查
			_, errPing := cli.Agent().Self()
			if errPing == nil {
				return &ConsulHelper{client: cli}, nil
			}
			lastErr = errPing
		} else {
			lastErr = err
		}
	}
	return nil, fmt.Errorf("all consul addresses failed: %v", lastErr)
}

// RegisterMatchEngine 注册撮合引擎服务到 Consul
// nodeID: 唯一节点ID，symbols: 该节点负责的交易对列表
func (c *ConsulHelper) RegisterMatchEngine(nodeID string, symbols []string, port int) error {
	reg := &api.AgentServiceRegistration{
		ID:   nodeID,
		Name: "match_engine",
		Port: port,
		Tags: symbols,
		Check: &api.AgentServiceCheck{
			TCP:      fmt.Sprintf("127.0.0.1:%d", port),
			Interval: "10s",
			Timeout:  "2s",
		},
	}
	return c.client.Agent().ServiceRegister(reg)
}

// DiscoverMatchEngine 查询负责 symbol 的撮合节点
func (c *ConsulHelper) DiscoverMatchEngine(symbol string) ([]*api.AgentService, error) {
	services, err := c.client.Agent().Services()
	if err != nil {
		return nil, err
	}
	var result []*api.AgentService
	for _, svc := range services {
		if svc.Service == "match_engine" {
			for _, tag := range svc.Tags {
				if tag == symbol {
					result = append(result, svc)
				}
			}
		}
	}
	return result, nil
}

// Client 返回 consul client
func (c *ConsulHelper) Client() *api.Client {
	return c.client
}
