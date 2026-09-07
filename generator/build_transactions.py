"""造交易（事实数据）。

维度 ID 不再自己凭空造，而是从 MinIO 维度表里取真实存在的 ID：
    1. load_dimension_pools() 启动时从 MinIO 读一次维度进内存
    2. make_transaction() 从内存池里挑 ID，保证交易的 ID 在维表里都能查到

用法（先建池，再造交易）：
    pools = load_dimension_pools()
    txn = make_transaction(pools)
"""
import decimal
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

import pandas as pd

from build_dimensions import (
    read_parquet,
    s3_client,
    write_parquet,
    load_customer,
    load_account,
    load_merchant,
)
from schema import Currency, Transaction, TransactionType

# 数据生成模式：batch=批量补历史 / realtime=实时流
GenMode = Literal["batch", "realtime"]

# ---- 交易事实表在 MinIO 里的位置（独立于 dim 桶）----
FACT_BUCKET = "fact"
KEY_TRANSACTION = "fact_transaction.parquet"

def load_dimension_pools() -> dict:
    client = s3_client()
    cust_df = load_customer(client)
    acc_df = load_account(client)
    merc_df = load_merchant(client)
    # 预先整理好：客户池、商户池、客户→账户 映射
    accounts_by_customer: dict[str, dict[str,datetime]] = {}
    for cid, aid, open_time in zip(acc_df["customer_id"], acc_df["account_id"],acc_df["open_time"]):
        if cid not in accounts_by_customer:
            accounts_by_customer[cid]={}
        accounts_by_customer[cid][aid]=open_time
    return {
        "customers": cust_df["customer_id"].tolist(),
        "merchants": merc_df["merchant_id"].tolist(),
        "accounts_by_customer": accounts_by_customer,
    }


def make_transaction(pools: dict, mode: GenMode = "realtime") -> Transaction:
    """随机造一条合法交易，ID 全部来自 MinIO 维度池。

    mode: 数据生成模式
        batch    批量：event_time = 开户时间 ~ 当前时间之间的随机时刻（补历史数据）
        realtime 实时：event_time = 当前时间往前 0~5 秒（制造乱序，给 Flink Watermark）
    """
    # 金额：10.00 ~ 9999.99，两位小数。用 Decimal，钱不用 float
    amount = decimal.Decimal(str(round(random.uniform(10, 9999.99), 2)))

    # 先挑客户 → 从他自己的账户里挑一个 → 保证 account 属于该 customer
    customer_id = random.choice(pools["customers"])
    account_id = random.choice(list(pools["accounts_by_customer"][customer_id]))
    merchant_id = random.choice(pools["merchants"])

    if mode == "batch":
        # 批量：在【开户时间 → 当前时间】区间内取随机时刻
        now = datetime.now(timezone.utc)
        open_time = pools["accounts_by_customer"][customer_id][account_id]
        span_seconds = (now - open_time).total_seconds()   # 开户到现在的总秒数
        if span_seconds > 0:
            event_time = open_time + timedelta(seconds=random.uniform(0, span_seconds))
        else:
            # 兜底：开户时间晚于当前时间（理论上不会发生），退化为实时逻辑
            event_time = now - timedelta(seconds=random.uniform(0, 5))
    else:
        # 实时：当前时间往前拨 0~5 秒 → 偶尔比上一条早，形成乱序
        event_time = datetime.now(timezone.utc) - timedelta(seconds=random.uniform(0, 5))
        # 测试late表：event_time = 当前时间往前 2 分钟
        # 测试方法：
        # 第 1 步：正常造数 1~2 分钟
        # 第 2 步：停止造数，等 30 秒
        # 第 3 步：补发 1 条 2 分钟前的消息 切换为以下的代码 
        # 第 4 步：等 5~10 秒，查 late 表
        # event_time = datetime.now(timezone.utc) - timedelta(minutes =2)

    # 截到毫秒：Flink json ISO-8601 只认到毫秒，6 位微秒会解析失败
    event_time = event_time.replace(microsecond=event_time.microsecond // 1000 * 1000)

    return Transaction(
        transaction_id=f"T{uuid.uuid4().hex[:16]}",
        amount=amount,
        currency=random.choice(list(Currency)),
        customer_id=customer_id,
        account_id=account_id,
        merchant_id=merchant_id,
        transaction_type=random.choice(list(TransactionType)),
        event_time=event_time,
    )


def make_batch_transaction(pools: dict, count: int) -> list[Transaction]:
    """批量造 count 条：event_time 在开户时间 ~ 当前时间之间随机（补历史数据用）。"""
    return [make_transaction(pools, mode="batch") for _ in range(count)]


def make_realtime_transaction(pools: dict) -> Transaction:
    """实时模式造一条：event_time = 当前时间往前 0~5 秒（实时流 + 乱序）。"""
    return make_transaction(pools, mode="realtime")


def save_transactions_to_minio(txns: list[Transaction], override: bool = False) -> None:
    """交易流水写成 Parquet 上传 MinIO；默认追加到旧数据后面，override=True 覆盖重来。"""
    client = s3_client()
    df_new = pd.DataFrame([t.model_dump() for t in txns])   # 模型 → dict → DataFrame

    # put_object 是覆盖写，追加 = 读旧数据 + concat + 写回
    old = read_parquet(client, KEY_TRANSACTION, bucket=FACT_BUCKET)
    df = df_new if old is None or override else pd.concat([old, df_new], axis=0, ignore_index=True)

    write_parquet(client, df, KEY_TRANSACTION, bucket=FACT_BUCKET)
    print(f"【{'覆盖' if override else '追加'}】写入 MinIO {FACT_BUCKET}/{KEY_TRANSACTION}："
          f"本次 {len(df_new)} 条，文件共 {len(df)} 条")


if __name__ == "__main__":
    pools = load_dimension_pools()
    txns = make_batch_transaction(pools, count=90000)
    save_transactions_to_minio(txns)
