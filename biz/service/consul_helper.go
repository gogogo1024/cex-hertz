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

// RegisterMatchEngine 注册撮合引擎服务到 Consul
// nodeID: 唯一节点ID，pairs: 该节点负责的交易对列表
func (c *ConsulHelper) RegisterMatchEngine(nodeID string, pairs []string, port int) error {
	reg := &api.AgentServiceRegistration{
		ID:   nodeID,
		Name: "match_engine",
		Port: port,
		Tags: pairs,
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
