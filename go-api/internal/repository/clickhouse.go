// Package repository — ClickHouse 查询封装（对齐 app/db.py 的 run_query_clickhouse）
// driver: github.com/ClickHouse/clickhouse-go/v2
//
// 用 clickhouse.OpenDB(&Options{Protocol: HTTP}) 拿到 *sql.DB，
// 复用 database/sql 标准 Scan 流程，与 StarRocks 代码结构完全对称。
package repository

import (
	"context"
	"fmt"
	"go-api/internal/config"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
)

type ClickHouse struct {
	DB *sqlx.DB
}

// NewClickHouse 用 HTTP 协议（端口 8123）连接 ClickHouse
func NewClickHouse(cfg *config.Config, timeout time.Duration) (*ClickHouse, error) {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.ClickHouseConfig.Host, cfg.ClickHouseConfig.HTTPPort)},
		Auth: clickhouse.Auth{
			Username: cfg.ClickHouseConfig.User,
			Password: cfg.ClickHouseConfig.Password,
			Database: cfg.ClickHouseConfig.Database,
		},
		Protocol:    clickhouse.HTTP,
		DialTimeout: timeout,
		Settings: clickhouse.Settings{
			"max_execution_time": timeout.Seconds(),
		},
	}
	slog.Info("clickhouse config dump",
		"addr", opts.Addr,
		"protocol", opts.Protocol,
		"database", cfg.ClickHouseConfig.Database,
	)
	db := clickhouse.OpenDB(opts)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("clickhouse ping failed: %w", err)
	}
	// clickhouse.OpenDB 返回 *sql.DB，包一层 sqlx.NewDb 拿到 *sqlx.DB
	return &ClickHouse{DB: sqlx.NewDb(db, "clickhouse")}, nil
}

// Select —— 查多条，与 StarRocks.Select 对称
//
//	dest 必须是 *[]T（切片指针）
//	想要编译期类型检查，用包级泛型函数 SelectRows
func (c *ClickHouse) Select(ctx context.Context, dest any, query string, args ...any) error {
	return c.DB.SelectContext(ctx, dest, query, args...)
}

// Get —— 查单条，与 StarRocks.Get 对称
//
//	dest 必须是 *T（结构体指针）
//	查不到返回 nil（sql.ErrNoRows 被吞掉）
func (c *ClickHouse) Get(ctx context.Context, dest any, query string, args ...any) error {
	return c.DB.GetContext(ctx, dest, query, args...)

}

// Query —— 通用查询，返回 []map[string]any（调试/临时用，正式业务请用 Select）
func (c *ClickHouse) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := c.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("clickhouse columns failed: %w", err)
	}
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range cols {
		ptrs[i] = &values[i]
	}
	results := make([]map[string]any, 0)
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("clickhouse scan failed: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i := range cols {
			row[cols[i]] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse rows err: %w", err)
	}
	return results, nil
}

func (c *ClickHouse) Close() error {
	return c.DB.Close()
}
