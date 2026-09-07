"""交易相关接口。

拆分说明见 docs/knowledge/fastapi/router.md：
- 这里用 APIRouter（不是 FastAPI 实例）
- main.py 用 app.include_router 挂上去
"""

from fastapi import APIRouter

from app.db import run_query
from app.schemas import TransactionOut

router = APIRouter()


@router.get("/transactions", response_model=list[TransactionOut])
def list_transactions(
    customer_id: str | None = None,   # 查询参数：/transactions?customer_id=C123（可选）
    limit: int = 20,                  # 查询参数：默认返回 20 条
) -> list[dict]:
    """查交易明细，可按 customer_id 过滤，limit 限制条数。

    TODO(你写)：
      1. 写 SQL 查 finance.dwd_transaction，按 event_time 倒序，取最新的
      2. 用命名参数 :customer_id / :limit，不要手动拼字符串（防 SQL 注入）
      3. customer_id 为 None 时不加 WHERE 条件（查全部）
      4. return run_query(sql, params)
    """
    sql_str = """
    SELECT
          transaction_id,
          event_time,
          customer_id,
          account_id,
          merchant_id,
          amount,
          currency,
          transaction_type
    FROM
        finance.dwd_transaction_online"""
    if customer_id:
        sql_str += " WHERE customer_id = :customer_id"
    sql_str += """ ORDER BY
        event_time DESC
    LIMIT
        :limit
    """
    params = {"limit": limit}
    if customer_id:
        params["customer_id"] = customer_id
    return run_query(sql_str, params)
