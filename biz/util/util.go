package util

import (
	"cex-hertz/conf"
)

// IsLocalMatchEngine 判断本节点是否负责该symbol
func IsLocalMatchEngine(symbol string) bool {
	cfg := conf.GetConf()
	return isLocalSymbol(symbol, cfg)
}

// isLocalSymbol 判断 symbol 是否属于本节点
func isLocalSymbol(symbol string, cfg *conf.Config) bool {
	for _, p := range ParsePairs(cfg.MatchEngine.MatchPairs) {
		if p == symbol {
			return true
		}
	}
	return false
}

// ParsePairs 工具函数，解析逗号分隔的交易对字符串
func ParsePairs(s string) []string {
	var res []string
	for _, p := range splitAndTrim(s, ",") {
		if p != "" {
			res = append(res, p)
		}
	}
	return res
}

func splitAndTrim(s, sep string) []string {
	var res []string
	for _, v := range split(s, sep) {
		res = append(res, trim(v))
	}
	return res
}

func split(s, sep string) []string {
	var res []string
	for _, v := range []byte(s) {
		if string(v) == sep {
			res = append(res, "")
		} else {
			if len(res) == 0 {
				res = append(res, "")
			}
			res[len(res)-1] += string(v)
		}
	}
	return res
}

func trim(s string) string {
	return string([]byte(s))
}
