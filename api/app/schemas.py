"""接口的出入参模型（Pydantic）。

作用：约束请求/响应的结构和类型，FastAPI 自动校验 + 自动生成 /docs 文档。
字段对齐 StarRocks 表 finance.dwd_transaction。
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class TransactionOut(BaseModel):
    """单条交易明细的返回结构（对应表 dwd_transaction 一行）。"""

    transaction_id: str
    event_time: datetime
    customer_id: str
    account_id: str
    merchant_id: str
    amount: Decimal
    currency: str
    transaction_type: str
class CustomerStat(BaseModel):
    """客户统计信息的返回结构（对 dwd_transaction 按客户聚合的一行）。"""
    customer_id: str
    amount_sum: Decimal
    transaction_count: int
    transaction_type: str


class CustomerProfile(BaseModel):
    """客户离线画像（来自 ads_customer_profile，T+1 快照）。"""
    dt: datetime
    customer_id: str
    txn_count: int
    total_amount: Decimal
    avg_ticket_amount: float
    refund_txn_rate: float
    has_refund: bool
    amount_tier: str
    ma7_total_amount: Decimal | None = None
    ma7_txn_count: int | None = None
    dod_amount_change: Decimal | None = None


class CustomerFullProfile(BaseModel):
    """客户全量画像：实时统计 + 离线画像（流批汇聚）。"""
    customer_id: str
    realtime_stats: list[CustomerStat]    # 来自 dwd_transaction_online（实时链路）
    offline_profile: CustomerProfile | None  # 来自 ads_customer_profile（离线链路，T+1）