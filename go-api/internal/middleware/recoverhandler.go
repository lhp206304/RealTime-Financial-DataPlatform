// Package middleware — panic 恢复：不崩进程，统一 500（Python 版没有，新增）
//
// 签名固定: func Recover(logger *slog.Logger) func(http.Handler) http.Handler
//
// TODO: handler 顶部 defer func(){ if p := recover(); p != nil { 记日志; http.Error(w,...,500) } }()
package middleware

import (
	"log/slog"
	"net/http"
)

func RecoverHandler(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if p := recover(); p != nil {
					logger.Error("http_panic",
						"method", r.Method,
						"path", r.URL.Path,
						"panic", p,
					)
					http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
