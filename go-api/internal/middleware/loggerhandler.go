// Package middleware — 请求日志：method / path / status / 耗时（Python 版没有，新增）
package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

// statusRecorder — 包一层 http.ResponseWriter，偷偷把 status code 记下来
// 原生接口没暴露 status，所以必须自己写这个包装器
type statusRecorder struct {
	http.ResponseWriter      // 嵌入原生的，它负责真正写出去
	status              int  // 我们偷偷记的状态码
	wrote               bool // 有没有写过（防止 WriteHeader 被调多次）
}

// WriteHeader — 覆写原生的 WriteHeader，在写出去之前先记一笔
func (s *statusRecorder) WriteHeader(code int) {
	if !s.wrote {
		s.status = code
		s.wrote = true
	}
	s.ResponseWriter.WriteHeader(code) // 交给原生的真写
}

// Logger — 请求日志中间件
//
// 签名固定: func Logger(logger *slog.Logger) func(http.Handler) http.Handler
// 三层闭包：外层装 logger，中层装 next handler，内层每请求执行一次
func LoggerHandler(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// 用包装器替掉原生 w，这样才能在 next.ServeHTTP 之后读到 status
			rec := &statusRecorder{ResponseWriter: w, status: 200}
			next.ServeHTTP(rec, r)

			logger.Info("http_request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"cost", time.Since(start),
			)
		})
	}
}
