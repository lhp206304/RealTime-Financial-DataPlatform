// Package main — 程序入口（对齐 api/app/main.py）
//
// 功能：读配置 → 建两个连接池 → 组装 service → 注册路由 → 启动 :8001 → 优雅关闭
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"go-api/internal/config"
	"go-api/internal/logger"
	"go-api/internal/repository"
	"go-api/internal/router"
	"go-api/internal/service"
)

func main() {
	godotenv.Load("../deploy/.env") // 路径相对于 go-api/ 目录
	logger.Init()
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	// ====== 连接池（进程级单例，全生命周期共享；defer 保证退出时释放）======
	// StarRocks：实时库，MySQL 协议
	srRepo, err := repository.NewStarRocks(cfg)
	if err != nil {
		slog.Error("starrocks new failed", "err", err)
		os.Exit(1)
	}
	defer srRepo.Close()

	// ClickHouse：离线库，HTTP 协议；超时统一从 config 取（单一数据源，不硬编码）
	chRepo, err := repository.NewClickHouse(cfg, cfg.RuntimeParams.QueryTimeout)
	if err != nil {
		slog.Error("clickhouse new failed", "err", err)
		os.Exit(1)
	}
	defer chRepo.Close()

	// ====== 依赖注入组装：repository → service → handler → router ======
	// 依赖方向单向：router 知道 service，service 知道 repository，反过来不知道
	txSvc := service.NewTransactionService(srRepo)
	custSvc := service.NewCustomerService(srRepo, chRepo)
	// 优雅关闭：先注册信号监听（必须在阻塞操作之前）
	// os.Interrupt = Ctrl+C；syscall.SIGTERM = kill 默认信号
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 构造 Server（配超时）
	srv := &http.Server{
		Addr:              ":8001",
		Handler:           router.New(slog.Default(), txSvc, custSvc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 用 goroutine 启动监听——ListenAndServe 会阻塞，放 goroutine 里让主 goroutine 能等信号
	slog.Info("server starting", "addr", srv.Addr)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server listen failed", "err", err)
			os.Exit(1)
		}
	}()

	// 主 goroutine 阻塞等信号
	<-ctx.Done()
	slog.Info("shutting down server...")

	// 阶段一：优雅关闭——停止接新连接，等已有请求自然收尾
	// ShutdownTimeout 是"最多等多久"，超时就放弃（防止卡死的 handler 让进程永远退不了）
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.RuntimeParams.ShutdownTimeout)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("shutdown did not complete gracefully within timeout, forcing close",
			"timeout", cfg.RuntimeParams.ShutdownTimeout, "err", err)
		// 阶段二：强制关闭——直接断所有 TCP 连接，卡死的 handler 会收到连接断开错误被迫退出
		srv.Close()
	}
	slog.Info("server shutdown complete")
}
