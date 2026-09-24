// Package handler — GET /health → {"status":"healthy"}（对齐 main.py 的 health()）
package handler

import (
	"encoding/json"
	"net/http"
)

func Health(w http.ResponseWriter, r *http.Request) {
	// 贴快递盒标签：告诉客户端这是 JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}
