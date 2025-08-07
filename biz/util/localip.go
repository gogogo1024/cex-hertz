package util

import (
	"net"
	"os"
)

// GetLocalIP 优先从环境变量 POD_IP/HOST_IP/SERVICE_HOST 获取，否则自动探测本地内网IP
func GetLocalIP() string {
	// 优先从常用环境变量获取
	if ip := os.Getenv("POD_IP"); ip != "" {
		return ip
	}
	if ip := os.Getenv("HOST_IP"); ip != "" {
		return ip
	}
	if ip := os.Getenv("SERVICE_HOST"); ip != "" {
		return ip
	}
	// 自动探测本地内网IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}
	return "127.0.0.1"
}
