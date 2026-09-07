
from fastapi import APIRouter, Path

from app.schemas import CustomerFullProfile, CustomerProfile, CustomerStat
from app.db import run_query


router = APIRouter()

@router.get("/customers/{customer_id}/full-profile", response_model=CustomerFullProfile)
def get_customer_full_profile(
    customer_id: str = Path(..., min_length=1)
) -> CustomerFullProfile:
    """流批汇聚：同时返回实时统计（dwd_transaction_online）+ 离线画像（ads_customer_profile）。"""
    # ① 实时：按交易类型聚合（来自 Flink Job1 实时写入的表）
    realtime_sql = """
    select
        customer_id,
        sum(amount) as amount_sum,
        count(1) as transaction_count,
        transaction_type
    from finance.dwd_transaction_online
    where customer_id = :customer_id
    group by customer_id, transaction_type
    """
    realtime_rows = run_query(realtime_sql, {"customer_id": customer_id})

    # ② 离线：最近一天的客户画像（来自 PySpark T+1 写入的表）
    offline_sql = """
    select
        dt, customer_id, txn_count, total_amount,
        avg_ticket_amount, refund_txn_rate, has_refund, amount_tier,
        ma7_total_amount, ma7_txn_count, dod_amount_change
    from finance.ads_customer_profile
    where customer_id = :customer_id
    order by dt desc
    limit 1
    """
    offline_rows = run_query(offline_sql, {"customer_id": customer_id})

    return CustomerFullProfile(
        customer_id=customer_id,
        realtime_stats=[CustomerStat(**row) for row in realtime_rows],
        offline_profile=CustomerProfile(**offline_rows[0]) if offline_rows else None,
    )
