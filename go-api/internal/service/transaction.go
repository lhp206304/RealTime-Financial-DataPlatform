// Package service — 交易查询业务（对齐 routers/transactions.py 的 list_transactions）
//
// TODO:
//  1. type TransactionService struct{ sr *repository.StarRocks }
//  2. func (s *TransactionService) List(ctx, customerID string, limit int) ([]model.TransactionOut, error)
//     SQL: 查 dwd_transaction_online，customer_id 可选过滤，ORDER BY event_time DESC LIMIT ?
//     ctx 超时由 handler 层负责，这里只管查询和行扫描
package service

import (
	"context"
	"fmt"
	"go-api/internal/model"
	"go-api/internal/repository"
)

type TransactionService struct {
	sr *repository.StarRocks
}

// NewTransactionService —— 构造函数：跨包组装入口（字段未导出，外部包只能通过它创建）
func NewTransactionService(sr *repository.StarRocks) *TransactionService {
	return &TransactionService{sr: sr}
}

// ListByCustomer —— 查指定客户的在线交易，按事件时间倒序
//
// SQL 由 service 层决定，repository 只负责执行
// 这样换表、加字段、改 WHERE 条件都只动 service，不动 repository
func (s *TransactionService) ListByCustomer(ctx context.Context, customerID string, limit int) ([]model.TransactionOnline, error) {
	var txs []model.TransactionOnline
	// LIMIT 不能用 ? 占位：StarRocks 不支持参数化 LIMIT/OFFSET（Error 1064）
	// 拼接是安全的：limit 是 int 类型且 handler 层已校验 >0，不存在注入面
	// customer_id 仍用 ? 参数化——它是外部输入字符串，绝不能拼
	err := repository.SelectRows(ctx, s.sr.DB, &txs,
		fmt.Sprintf("SELECT * FROM finance.dwd_transaction_online WHERE customer_id = ? ORDER BY event_time DESC LIMIT %d", limit),
		customerID,
	)
	if err != nil {
		return nil, fmt.Errorf("list transaction online failed: %w", err)
	}
	return txs, nil
}
