// Package service — 客户全量画像（对齐 routers/customers.py）
// ★ 本次学习核心：errgroup 并发查两库
//
// TODO:
//  1. type CustomerService struct{ sr *repository.StarRocks; ch *repository.ClickHouse }
//  2. func (s *CustomerService) FullProfile(ctx, customerID string) (*model.CustomerFullProfile, error)
//  3. golang.org/x/sync/errgroup:
//     g, gctx := errgroup.WithContext(ctx)
//     goroutine A: gctx 查 StarRocks → 按 transaction_type 聚合 → realtimeStats
//     goroutine B: gctx 查 ClickHouse ads_customer_profile 最新一天 → offlineProfile
//     g.Wait() 等齐；任一出错 gctx 自动取消另一路
//  4. 两个 goroutine 各写各的局部变量，Wait 后再合并，不要共享写同一变量
package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"go-api/internal/model"
	"go-api/internal/repository"
)

type CustomerService struct {
	sr *repository.StarRocks
	ch *repository.ClickHouse
}

// NewCustomerService —— 构造函数：跨包组装入口（字段未导出，外部包只能通过它创建）
func NewCustomerService(sr *repository.StarRocks, ch *repository.ClickHouse) *CustomerService {
	return &CustomerService{sr: sr, ch: ch}
}

func (s *CustomerService) FullProfile(ctx context.Context, customerID string) (*model.CustomerFullProfile, error) {
	g, gctx := errgroup.WithContext(ctx)
	realtimeStats := []model.CustomerStat{}
	// 开goroutine A: 查 实时数据： StarRocks → 按 transaction_type 聚合 → realtimeStats
	g.Go(func() error {
		err := repository.SelectRows(gctx, s.sr.DB, &realtimeStats,
			`select
						customer_id,
						sum(amount) as amount_sum,
						count(1) as transaction_count,
						transaction_type
					from finance.dwd_transaction_online
					where customer_id = ?
					group by customer_id, transaction_type `,
			customerID,
		)
		if err != nil {
			return fmt.Errorf("starrocks query failed: %w", err)
		}
		return nil
	})
	var offlineProfile *model.CustomerProfile // nil = ClickHouse 最新快照不存在
	// 开goroutine B: 查 离线数据：ClickHouse ads_customer_profile 最新一天 → offlineProfile
	g.Go(func() error {
		// 先查进局部变量 p，成功才把指针交给 offlineProfile
		// 这样 offlineProfile == nil 严格等价于"查不到"，不会出现零值假数据
		var p model.CustomerProfile
		err := repository.GetRow(gctx, s.ch.DB, &p,
			`select
				dt, customer_id, txn_count, total_amount,
				avg_ticket_amount, refund_txn_rate, has_refund, amount_tier,
				ma7_total_amount, ma7_txn_count, dod_amount_change
			from finance.ads_customer_profile
			where customer_id = ?
			order by dt desc limit 1 `,
			customerID,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // 查不到是正常业务分支：T+1 快照当天未生成 → offline 保持 nil
		}
		if err != nil {
			return fmt.Errorf("clickhouse query failed: %w", err)
		}
		offlineProfile = &p
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return &model.CustomerFullProfile{
		CustomerID:     customerID,
		RealtimeStats:  realtimeStats,
		OfflineProfile: offlineProfile, // nil → JSON 里 offline_profile 字段整个省略（omitempty）
	}, nil
}
