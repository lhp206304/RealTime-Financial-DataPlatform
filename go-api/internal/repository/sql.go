// Package repository — 包级泛型查询函数，StarRocks / ClickHouse 共用
//
// Go 不允许方法有独立类型参数（method must have no type parameters），
// 所以泛型逻辑放在包级函数里，两个库的便捷方法（Select / Get）内部直接调 sqlx。
//
// 调用方二选一：
//
//	便捷方法：sr.Select(ctx, &txs, query, args...)             — dest 是 any，运行时检查
//	泛型函数：SelectRows(ctx, sr.DB, &txs, query, args...)    — dest 是 *[]T，编译期检查
package repository

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// SelectRows —— 查多条，泛型约束 dest 必须是 *[]T
//
//	✅ SelectRows(ctx, sr.DB, &txs, "SELECT ...")    // T 从 &txs 自动推断
//	❌ SelectRows(ctx, sr.DB, txs, "SELECT ...")     // 编译报错：txs 是 []T 不是 *[]T
//	❌ SelectRows(ctx, sr.DB, &tx, "SELECT ...")     // 编译报错：&tx 是 *T 不是 *[]T
func SelectRows[T any](ctx context.Context, db *sqlx.DB, dest *[]T, query string, args ...any) error {
	return db.SelectContext(ctx, dest, query, args...)
}

// GetRow —— 查单条，泛型约束 dest 必须是 *T
//
// 查不到时透传 sql.ErrNoRows（哨兵错误），由调用方决定怎么处理：
//
//	errors.Is(err, sql.ErrNoRows)   // 无记录是正常业务分支时（如 T+1 快照未生成）
//	err != nil                      // 无记录也算异常时
//
//	✅ GetRow(ctx, sr.DB, &tx, "SELECT ... WHERE id = ?", id)
//	❌ GetRow(ctx, sr.DB, tx, "SELECT ...")           // 编译报错：tx 是 T 不是 *T
func GetRow[T any](ctx context.Context, db *sqlx.DB, dest *T, query string, args ...any) error {
	return db.GetContext(ctx, dest, query, args...)
}
