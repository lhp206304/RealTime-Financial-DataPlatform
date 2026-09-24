// // Package repository — StarRocks 查询封装（对齐 app/db.py 的 run_query）
// // driver: github.com/go-sql-driver/mysql（StarRocks 兼容 MySQL 协议）
// //
// // TODO:
// //  1. type StarRocks struct{ DB *sql.DB }
// //  2. func NewStarRocks(cfg): sql.Open("mysql", DSN) + Ping
// //     DSN 模板: user:pass@tcp(host:port)/db?parseTime=true&readTimeout=5s
// //  3. 连接池: SetMaxOpenConns(10) / SetMaxIdleConns(5) / SetConnMaxLifetime(time.Hour)
// //  4. func (s *StarRocks) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error)
// //     必须用 QueryContext，ctx 从 handler 层带超时传下来
// //  5. func (s *StarRocks) Close() error
package repository

// import (
// 	"context"
// 	"database/sql"
// 	"fmt"
// 	"go-api/internal/config"
// 	"go-api/internal/model"
// 	"log/slog"
// 	"time"

// 	_ "github.com/go-sql-driver/mysql"
// )

// type StarRocks struct {
// 	DB *sql.DB
// }

// // 新增一个StarRocks连接池
// func NewStarRocks(cfg *config.Config) (*StarRocks, error) {
// 	slog.Info(
// 		"starrocks config dump",
// 		"host", cfg.StarRocksConfig.HostLocal,
// 		"port", cfg.StarRocksConfig.MysqlPort,
// 		"user", cfg.StarRocksConfig.User,
// 		"database", cfg.StarRocksConfig.Database,
// 	)
// 	// 配置数据库的连接
// 	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&readTimeout=%s",
// 		cfg.StarRocksConfig.User,
// 		cfg.StarRocksConfig.Password,
// 		cfg.StarRocksConfig.HostLocal,
// 		cfg.StarRocksConfig.MysqlPort,
// 		cfg.StarRocksConfig.Database,
// 		cfg.RuntimeParams.QueryTimeout,
// 	)
// 	db, err := sql.Open("mysql", dsn)
// 	if err != nil {
// 		return nil, fmt.Errorf("starrocks open failed : %w", err)
// 	}
// 	db.SetMaxOpenConns(10)
// 	db.SetMaxIdleConns(5)
// 	db.SetConnMaxLifetime(time.Hour)
// 	if err := db.Ping(); err != nil {
// 		return nil, fmt.Errorf("starrocks ping failed : %w", err)
// 	}
// 	return &StarRocks{DB: db}, nil
// }

// // Query —— 通用查询：返回 []map[string]any，调试/临时查询用
// //
// // 注意：map[string]any 里的 any 是 driver 运行时类型（decimal → []byte、
// // datetime → time.Time），要手动类型断言。业务查询请用 List 这种直接扫 struct 的方法。
// func (s *StarRocks) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
// 	rows, err := s.DB.QueryContext(ctx, query, args...)
// 	if err != nil {
// 		return nil, fmt.Errorf("starrocks query failed : %w", err)
// 	}
// 	defer rows.Close()
// 	cols, err := rows.Columns()
// 	if err != nil {
// 		return nil, fmt.Errorf("fetch starrocks columns failed : %w", err)
// 	}
// 	values := make([]any, len(cols))
// 	ptr := make([]any, len(cols))
// 	resules := make([]map[string]any, 0)
// 	for i := range cols {
// 		ptr[i] = &values[i]
// 	}
// 	// 遍历每行
// 	for rows.Next() {
// 		// 把行数据装到切片values
// 		if err := rows.Scan(ptr...); err != nil {
// 			return nil, fmt.Errorf("starrocks scan failed : %w", err)
// 		}
// 		// 构建每行数据
// 		row := make(map[string]any, len(cols))
// 		for i := range cols {
// 			row[cols[i]] = values[i]
// 		}
// 		resules = append(resules, row)
// 	}
// 	//判断行遍历是否正常退出
// 	if err := rows.Err(); err != nil {
// 		return nil, fmt.Errorf("starrocks rows next failed : %w", err)
// 	}

// 	return resules, nil
// }

// // List —— 业务查询：直接扫到 TransactionOnline 结构体，不走 map 中转
// //
// // 为什么直接 rows.Scan 扫 struct 字段，而不是先 Query 返回 map 再转？
// //  1. map[string]any 里的 any 是 driver 运行时类型（decimal → []byte、
// //     datetime → time.Time），要手动断言再赋值，繁琐且易出错
// //  2. 直接 rows.Scan(&tx.Field1, &tx.Field2, ...) 让 database/sql 帮你做类型转换，
// //     类型不匹配直接编译报错，零值自动填
// //  3. 少一次中间遍历，性能略好
// //
// // 注意：Scan 参数顺序必须和 SELECT 列顺序一致，不能乱。
// //
// //	用 SELECT * 时列顺序 = DDL 定义顺序（TransactionOnline 字段顺序已对齐 DDL）。
// //	如果显式写了 SELECT col1, col3，那 Scan 顺序也必须跟着改成 &tx.Col1, &tx.Col3。
// func (s *StarRocks) List(ctx context.Context, customerID string, limit int) ([]model.TransactionOnline, error) {
// 	rows, err := s.DB.QueryContext(ctx,
// 		"SELECT * FROM finance.dwd_transaction_online WHERE customer_id = ? ORDER BY event_time DESC LIMIT ?",
// 		customerID, limit,
// 	)
// 	if err != nil {
// 		return nil, fmt.Errorf("starrocks list query failed : %w", err)
// 	}
// 	defer rows.Close()

// 	txs := make([]model.TransactionOnline, 0)
// 	for rows.Next() {
// 		var tx model.TransactionOnline
// 		// 按 SELECT * 的列顺序（= DDL 字段顺序）把每列扫到对应字段指针
// 		if err := rows.Scan(
// 			&tx.TransactionID,
// 			&tx.EventTime,
// 			&tx.CustomerID,
// 			&tx.AccountID,
// 			&tx.MerchantID,
// 			&tx.Amount,
// 			&tx.Currency,
// 			&tx.TransactionType,
// 			&tx.CustomerLevel, // NULL 列 → 扫进 *string，驱动自动把 NULL 变成 nil
// 			&tx.CustomerRegion,
// 			&tx.CustomerRegisterTime,
// 			&tx.MerchantCategory,
// 			&tx.MerchantRiskLevel,
// 		); err != nil {
// 			return nil, fmt.Errorf("starrocks scan transaction failed : %w", err)
// 		}
// 		txs = append(txs, tx)
// 	}
// 	if err := rows.Err(); err != nil {
// 		return nil, fmt.Errorf("starrocks rows iterate failed : %w", err)
// 	}
// 	return txs, nil
// }

// func (s *StarRocks) Close() error {
// 	return s.DB.Close()
// }
