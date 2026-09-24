// Package model — 响应结构体（对齐 api/app/schemas.py）
//
// TODO: 定义 4 个 struct，json tag 用 snake_case
//
//	TransactionOut / CustomerStat / CustomerProfile / CustomerFullProfile
//	金额字段建议用 shopspring/decimal（项目约束），JSON 输出仍是数字
package model

import (
	"time"

	"github.com/shopspring/decimal"
)

// TransactionOnline — 逐列对齐 DWD 表 dwd_transaction_online 的 DDL
//
// tag 说明：
//   - db:"xxx"  → sqlx 读这个 tag 匹配 SQL 列名和字段名（没有的话自动 snake_case 转换）
//   - json:"xxx" → encoding/json 读这个 tag 决定 JSON 输出字段名
//     两个 tag 可以共存，互不干扰，分别被各自的库读取
//
// 类型映射规则：
//   - NOT NULL varchar → string
//   - NOT NULL datetime → time.Time（DSN 里必须 parseTime=true，驱动才会把 datetime 扫成 time.Time）
//   - NOT NULL decimal → decimal.Decimal（项目约束：金额禁用 float64，二进制浮点算钱会丢精度）
//   - NULL 列 → 指针 *string：nil = 数据库 NULL，"" = 空字符串，两者语义不同，指针才能区分
type TransactionOnline struct {
	TransactionID        string          `db:"transaction_id" json:"transaction_id"`
	EventTime            time.Time       `db:"event_time" json:"event_time"`
	CustomerID           string          `db:"customer_id" json:"customer_id"`
	AccountID            string          `db:"account_id" json:"account_id"`
	MerchantID           string          `db:"merchant_id" json:"merchant_id"`
	Amount               decimal.Decimal `db:"amount" json:"amount"`
	Currency             string          `db:"currency" json:"currency"`
	TransactionType      string          `db:"transaction_type" json:"transaction_type"`
	CustomerLevel        *string         `db:"customer_level" json:"customer_level"`
	CustomerRegion       *string         `db:"customer_region" json:"customer_region"`
	CustomerRegisterTime *string         `db:"customer_register_time" json:"customer_register_time"`
	MerchantCategory     *string         `db:"merchant_category" json:"merchant_category"`
	MerchantRiskLevel    *string         `db:"merchant_risk_level" json:"merchant_risk_level"`
}

// CustomerStat — 按交易类型聚合的客户实时统计（对齐 Python schemas.py CustomerStat）
//
// 来自 SQL: SELECT customer_id, sum(amount) AS amount_sum, count(1) AS transaction_count, transaction_type
// 聚合结果没有 NULL 列，全部用值类型
type CustomerStat struct {
	CustomerID       string          `db:"customer_id" json:"customer_id"`
	AmountSum        decimal.Decimal `db:"amount_sum" json:"amount_sum"`
	TransactionCount int             `db:"transaction_count" json:"transaction_count"`
	TransactionType  string          `db:"transaction_type" json:"transaction_type"`
}

// CustomerProfile — 客户离线画像（来自 ClickHouse ads_customer_profile，T+1 快照）
// 对齐 Python schemas.py CustomerProfile
//
// 可选列（ma7_*、dod_amount_change）是 ClickHouse 里可能为 NULL 的指标字段 → 用指针
type CustomerProfile struct {
	Dt              time.Time        `db:"dt" json:"dt"`
	CustomerID      string           `db:"customer_id" json:"customer_id"`
	TxnCount        int              `db:"txn_count" json:"txn_count"`
	TotalAmount     decimal.Decimal  `db:"total_amount" json:"total_amount"`
	AvgTicketAmount float64          `db:"avg_ticket_amount" json:"avg_ticket_amount"`
	RefundTxnRate   float64          `db:"refund_txn_rate" json:"refund_txn_rate"`
	HasRefund       bool             `db:"has_refund" json:"has_refund"`
	AmountTier      string           `db:"amount_tier" json:"amount_tier"`
	Ma7TotalAmount  *decimal.Decimal `db:"ma7_total_amount" json:"ma7_total_amount,omitempty"`
	Ma7TxnCount     *int             `db:"ma7_txn_count" json:"ma7_txn_count,omitempty"`
	DodAmountChange *decimal.Decimal `db:"dod_amount_change" json:"dod_amount_change,omitempty"`
}

// CustomerFullProfile — 客户全量画像：实时统计 + 离线画像（流批汇聚）
// 对齐 Python schemas.py CustomerFullProfile
//
// realtime_stats: 来自 StarRocks dwd_transaction_online 按交易类型聚合
// offline_profile: 来自 ClickHouse ads_customer_profile 最近一天快照，可能查不到 → 指针
type CustomerFullProfile struct {
	CustomerID     string           `json:"customer_id"`
	RealtimeStats  []CustomerStat   `json:"realtime_stats"`
	OfflineProfile *CustomerProfile `json:"offline_profile,omitempty"`
}
