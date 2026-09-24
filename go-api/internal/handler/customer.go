// Package handler — GET /customers/{customer_id}/full-profile
//
// 以当前代码为准（原 TODO 过时点）：
//   - offline 查不到的处理已由 model 层承担：OfflineProfile 是 *CustomerProfile 指针，
//     ClickHouse 查不到时 service 返回零值/nil，无需 handler 特判
package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"go-api/internal/service"
)

type CustomerHandler struct{ svc *service.CustomerService }

// NewCustomerHandler —— 构造函数：router 组装入口（字段未导出，跨包只能通过它创建）
func NewCustomerHandler(svc *service.CustomerService) *CustomerHandler {
	return &CustomerHandler{svc: svc}
}

// FullProfile — 客户全量画像（实时聚合 + 离线快照）
//
// 分层职责：路径参数提取 → 超时控制 → 调 service → 翻译成 HTTP 状态码
// 业务错误只在 handler 记日志（service/repository 只 return err，错误终点站原则）
func (h *CustomerHandler) FullProfile(w http.ResponseWriter, r *http.Request) {
	// 1. 路径参数：对应路由注册里的 {customer_id} 占位符
	customerID := chi.URLParam(r, "customer_id")
	if customerID == "" {
		http.Error(w, "missing customer_id", http.StatusBadRequest)
		return
	}

	// 2. 超时控制：handler 层负责（service 不感知）
	// 5 秒到点 ctx 自动取消 → errgroup 的 gctx 联动取消 → 两路查询提前返回
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// 3. 调 service（内部 errgroup 并发查 StarRocks + ClickHouse）
	profile, err := h.svc.FullProfile(ctx, customerID)
	if err != nil {
		// 统一日志位置：handler 拿得到完整请求上下文（method/path/customer_id）
		slog.Error("full_profile_failed",
			"method", r.Method,
			"path", r.URL.Path,
			"customer_id", customerID,
			"err", err,
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(profile)
}
