// Package handler — GET /transactions（对齐 FastAPI 的 query 参数处理）
//
// 以当前代码为准（原 TODO 过时点）：
//   - service 方法名是 ListByCustomer（不是 TODO 里的 List）
//   - 返回 []model.TransactionOnline（不是 TransactionOut）
//   - customer_id 必填：当前 SQL 是 WHERE customer_id = ?，不支持无过滤全表查
package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"go-api/internal/service"
)

type TransactionHandler struct{ svc *service.TransactionService }

// NewTransactionHandler —— 构造函数：router 组装入口（字段未导出，跨包只能通过它创建）
func NewTransactionHandler(svc *service.TransactionService) *TransactionHandler {
	return &TransactionHandler{svc: svc}
}

// List — 查询指定客户的在线交易列表
//
// Query 参数：
//   - customer_id：必填，缺失返回 400
//   - limit：可选，默认 20；非数字或 <=0 返回 400
//
// 分层职责：参数提取 → 超时控制 → 调 service → 翻译成 HTTP 状态码
// 业务错误只在 handler 记日志（service/repository 只 return err，错误终点站原则）
func (h *TransactionHandler) List(w http.ResponseWriter, r *http.Request) {
	// 1. customer_id 必填校验
	customerID := r.URL.Query().Get("customer_id")
	if customerID == "" {
		http.Error(w, "missing required query param: customer_id", http.StatusBadRequest)
		return
	}

	// 2. limit 校验：strconv.Atoi 把字符串转 int，失败说明用户传了非法值
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			http.Error(w, "invalid limit: must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = n
	}

	// 3. 超时控制：handler 层负责（service 不感知）
	// 5 秒到点 ctx 自动取消，底层 db.SelectContext 感知取消提前返回
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// 4. 调 service
	txs, err := h.svc.ListByCustomer(ctx, customerID, limit)
	if err != nil {
		// 统一日志位置：handler 拿得到完整请求上下文（method/path/参数），排查时能对上请求
		slog.Error("list_transactions_failed",
			"method", r.Method,
			"path", r.URL.Path,
			"customer_id", customerID,
			"limit", limit,
			"err", err,
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(txs)
}
