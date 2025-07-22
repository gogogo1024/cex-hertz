// package conf
//
// import (
//
//	"fmt"
//	"os"
//
// )
//
//	type Config struct {
//		PostgresConn  string
//		KafkaBrokers  []string
//		KafkaTopic    string
//		WebSocketAddr string
//		ConsulAddr    string
//		NodeID        string
//		MatchPairs    []string
//		MatchPort     int
//	}
//
//	func Load() *Config {
//		cfg := &Config{
//			PostgresConn:  getEnv("CEX_POSTGRES_CONN", "postgres://postgres:postgres@localhost:5432/cex?sslmode=disable"),
//			KafkaBrokers:  []string{getEnv("CEX_KAFKA_BROKER", "localhost:9092")},
//			KafkaTopic:    getEnv("CEX_KAFKA_TOPIC", "cex_trades"),
//			WebSocketAddr: getEnv("CEX_WS_ADDR", ":8080"),
//			ConsulAddr:    getEnv("CEX_CONSUL_ADDR", "127.0.0.1:8500"),
//			NodeID:        getEnv("CEX_NODE_ID", "node-1"),
//			MatchPairs:    parsePairs(getEnv("CEX_MATCH_PAIRS", "BTCUSDT,ETHUSDT")),
//			MatchPort:     getEnvInt("CEX_MATCH_PORT", 9000),
//		}
//		return cfg
//	}
//
//	func parsePairs(s string) []string {
//		var res []string
//		for _, p := range splitAndTrim(s, ",") {
//			if p != "" {
//				res = append(res, p)
//			}
//		}
//		return res
//	}
//
//	func splitAndTrim(s, sep string) []string {
//		var res []string
//		for _, v := range split(s, sep) {
//			res = append(res, trim(v))
//		}
//		return res
//	}
//
//	func split(s, sep string) []string {
//		return []string{os.ExpandEnv(s)}
//	}
//
//	func trim(s string) string {
//		return os.ExpandEnv(s)
//	}
//
//	func getEnv(key, def string) string {
//		if v := os.Getenv(key); v != "" {
//			return v
//		}
//		return def
//	}
//
//	func getEnvInt(key string, def int) int {
//		if v := os.Getenv(key); v != "" {
//			var i int
//			_, err := fmt.Sscanf(v, "%d", &i)
//			if err == nil {
//				return i
//			}
//		}
//		return def
//	}
package conf

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/kr/pretty"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

var (
	conf *Config
	once sync.Once
)

type Config struct {
	Env         string
	Hertz       Hertz       `yaml:"hertz"`
	MySQL       MySQL       `yaml:"mysql"`
	Redis       Redis       `yaml:"redis"`
	Postgres    Postgres    `yaml:"postgres"`
	Kafka       Kafka       `yaml:"kafka"`
	MatchEngine MatchEngine `yaml:"match_engine"`
	Registry    Registry    `yaml:"registry"`
}

type MySQL struct {
	DSN string `yaml:"dsn"`
}

type Redis struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	Username string `yaml:"username"`
	DB       int    `yaml:"db"`
}
type Postgres struct {
	DSN string `yaml:"dsn"`
}
type Kafka struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}
type Registry struct {
	RegistryAddress []string `yaml:"registry_address"`
	Username        string   `yaml:"username"`
	Password        string   `yaml:"password"`
}
type MatchEngine struct {
	NodeID     string `yaml:"node_id"`
	MatchPairs string `yaml:"match_pairs"`
	MatchPort  int    `yaml:"match_port"`
}
type Hertz struct {
	Service         string `yaml:"service"`
	Address         string `yaml:"address"`
	EnablePprof     bool   `yaml:"enable_pprof"`
	EnableGzip      bool   `yaml:"enable_gzip"`
	EnableAccessLog bool   `yaml:"enable_access_log"`
	LogLevel        string `yaml:"log_level"`
	LogFileName     string `yaml:"log_file_name"`
	LogMaxSize      int    `yaml:"log_max_size"`
	LogMaxBackups   int    `yaml:"log_max_backups"`
	LogMaxAge       int    `yaml:"log_max_age"`
	RegistryAddr    string `yaml:"registry_addr"`
	MetricsPort     string `yaml:"metrics_port"`
	WsPort          string `yaml:"ws_port"`
}

// GetConf gets configuration instance
func GetConf() *Config {
	once.Do(initConf)
	return conf
}

func initConf() {
	prefix := "conf"
	confFileRelPath := filepath.Join(prefix, filepath.Join(GetEnv(), "conf.yaml"))
	content, err := ioutil.ReadFile(confFileRelPath)
	if err != nil {
		panic(err)
	}

	conf = new(Config)
	err = yaml.Unmarshal(content, conf)
	if err != nil {
		hlog.Error("parse yaml error - %v", err)
		panic(err)
	}
	if err := validator.Validate(conf); err != nil {
		hlog.Error("validate config error - %v", err)
		panic(err)
	}

	conf.Env = GetEnv()

	pretty.Printf("%+v\n", conf)
}

func GetEnv() string {
	e := os.Getenv("GO_ENV")
	if len(e) == 0 {
		return "test"
	}
	return e
}

func LogLevel() hlog.Level {
	level := GetConf().Hertz.LogLevel
	switch level {
	case "trace":
		return hlog.LevelTrace
	case "debug":
		return hlog.LevelDebug
	case "info":
		return hlog.LevelInfo
	case "notice":
		return hlog.LevelNotice
	case "warn":
		return hlog.LevelWarn
	case "error":
		return hlog.LevelError
	case "fatal":
		return hlog.LevelFatal
	default:
		return hlog.LevelInfo
	}
}
