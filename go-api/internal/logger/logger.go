// Package logger — slog JSON 日志初始化（角色对齐 shared/log.py）
//
// TODO:
//  1. func Init(): slog.NewJSONHandler(os.Stdout, ...) + slog.SetDefault
//  2. 业务代码直接用 slog.Info / slog.Error（全局 default logger）
package logger

import (
	"os"

	"log/slog"
)

func Init() {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(h))
}
