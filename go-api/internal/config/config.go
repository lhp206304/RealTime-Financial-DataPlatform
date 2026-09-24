// Package config — 环境变量 → 结构体（对齐 api/app/settings.py）
//
// 硬性约束：连接信息无默认值，缺失时启动报错（参考 deploy/.env.example 的变量名）
//
// TODO:
//  1. type Config struct{ StarRocks StarRocksConfig; ClickHouse ClickHouseConfig }
//  2. 两个子 struct 分别用 envconfig 前缀 STARROCKS_ / CLICKHOUSE_ 各 Process 一次
//  3. QueryTimeout 保留默认值 5 秒（代码级参数，非连接信息）
//  4. func Load() (*Config, error)
package config

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type StarRocksConfig struct {
	Host      string `envconfig:"HOST"`       // 与 deploy/.env / docker-compose *conn-env 统一命名
	MysqlPort string `envconfig:"MYSQL_PORT"`
	User      string
	Password  string
	Database  string
}
type ClickHouseConfig struct {
	Host     string `envconfig:"HOST"`         // 与 deploy/.env / docker-compose *conn-env 统一命名
	HTTPPort string `envconfig:"HTTP_PORT"`
	User     string
	Password string
	Database string
}
type Config struct {
	StarRocksConfig
	ClickHouseConfig
	RuntimeParams RuntimeParams
}
type RuntimeParams struct {
	QueryTimeout    time.Duration `envconfig:"QUERY_TIMEOUT" default:"5s"`
	ShutdownTimeout time.Duration `envconfig:"SHUTDOWN_TIMEOUT" default:"30s"`
}

func Load() (*Config, error) {
	var (
		sr StarRocksConfig
		ch ClickHouseConfig
		rt RuntimeParams
	)

	if err := envconfig.Process("STARROCKS", &sr); err != nil {
		return nil, fmt.Errorf("failed to process STARROCKS env: %w", err)
	}
	if err := envconfig.Process("CLICKHOUSE", &ch); err != nil {
		return nil, fmt.Errorf("failed to process CLICKHOUSE env: %w", err)
	}
	if err := envconfig.Process("RUNTIME", &rt); err != nil {
		return nil, fmt.Errorf("failed to process RUNTIME params env: %w", err)
	}
	return &Config{
		sr,
		ch,
		rt,
	}, nil
}
