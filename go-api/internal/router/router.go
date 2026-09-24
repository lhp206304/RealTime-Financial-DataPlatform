// Package router — Chi 路由注册（对齐 main.py 的 include_router）
//
// 依赖注入方向：main 传 service → router 装 handler → handler 调 service
// router 不写业务，只负责「URL → handler」的映射和中间件编排
package router

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"go-api/internal/handler"
	mymw "go-api/internal/middleware" // 自研中间件与 chi 的 middleware 包重名，起别名区分
	"go-api/internal/service"
)

// New —— 组装路由；service 由 main 注入，router 不自己建连接池
func New(logger *slog.Logger, txSvc *service.TransactionService, custSvc *service.CustomerService) http.Handler {
	r := chi.NewRouter()

	// 全局中间件：Use 必须写在路由注册之前（chi 硬性契约，否则 panic）
	r.Use(mymw.LoggerHandler(logger), mymw.RecoverHandler(logger))

	// handler 实例化：通过构造函数注入 service（字段未导出，跨包必须走 New）
	txHandler := handler.NewTransactionHandler(txSvc)
	custHandler := handler.NewCustomerHandler(custSvc)

	// 路由注册
	r.Get("/health", handler.Health)
	r.Get("/transactions", txHandler.List) // ?customer_id=xxx&limit=20
	r.Get("/customers/{customer_id}/full-profile", custHandler.FullProfile)

	return r
}
