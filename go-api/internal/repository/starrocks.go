// Package repository — StarRocks 数据访问层
// driver: github.com/go-sql-driver/mysql（StarRocks 兼容 MySQL 协议）
// 增强: github.com/jmoiron/sqlx（自动按 db tag 映射列名 → struct 字段）
//
// 分层约定：
//
//	repository 只管「建连接池 + 提供通用查询入口 + Close」
//	service 层写具体 SQL、决定查什么表、按什么条件
//	→ repository 不硬编码任何表名/列名，SQL 全由调用方传入
//
// 包级泛型函数 SelectRows / GetRow 已在 sql.go 定义，StarRocks 和 ClickHouse 共用。
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"go-api/internal/config"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type StarRocks struct {
	DB *sqlx.DB
}

// NewStarRocks —— 建连接池
func NewStarRocks(cfg *config.Config) (*StarRocks, error) {
	slog.Info(
		"starrocks config dump",
		"host", cfg.StarRocksConfig.Host,
		"port", cfg.StarRocksConfig.MysqlPort,
		"user", cfg.StarRocksConfig.User,
		"database", cfg.StarRocksConfig.Database,
	)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&readTimeout=%s",
		cfg.StarRocksConfig.User,
		cfg.StarRocksConfig.Password,
		cfg.StarRocksConfig.Host,
		cfg.StarRocksConfig.MysqlPort,
		cfg.StarRocksConfig.Database,
		cfg.RuntimeParams.QueryTimeout,
	)
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("starrocks open failed : %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("starrocks ping failed : %w", err)
	}
	return &StarRocks{DB: db}, nil
}

// Select —— 查多条
//
//	dest 必须是 *[]T（切片指针），sqlx 按 db tag 映射列名到字段
//	SQL 和 args 由调用方（通常是 service 层）传入
//
// 注意：dest 签名是 any（Go 不允许方法有独立泛型参数），
// 传错类型运行时 sqlx 会报错。想要编译期检查，用包级泛型函数 SelectRows。
func (s *StarRocks) Select(ctx context.Context, dest any, query string, args ...any) error {
	return s.DB.SelectContext(ctx, dest, query, args...)
}

// Get —— 查单条
//
//	dest 必须是 *T（结构体指针）
//	查不到返回 nil（sql.ErrNoRows 被吞掉），上层用零值判断
func (s *StarRocks) Get(ctx context.Context, dest any, query string, args ...any) error {
	return s.DB.GetContext(ctx, dest, query, args...)

}

// Exec —— 写操作（INSERT / UPDATE / DELETE）
func (s *StarRocks) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.DB.ExecContext(ctx, query, args...)
}

func (s *StarRocks) Close() error {
	return s.DB.Close()
}
