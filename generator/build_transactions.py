"""造交易（事实数据）。

维度 ID 不凭空造，全部来自 MinIO 维度池：
    1. load_dimension_pools() 从 MinIO 读维度（只取 status=ACTIVE 的 customer/merchant）
    2. make_transaction() 从池里挑 ID，保证交易的 ID 在存活维表里都能查到

三种生成模式（GenMode）：
    batch    event_time 在开户时间 ~ 当前时间之间随机（补历史）
    realtime event_time = 当前时间往前 0~5 秒（实时乱序流）
    day      event_time 均匀落在指定 ds 当天（每日 Airflow 批造数）

用法：
    pools = load_dimension_pools()
    txn = make_transaction(pools, mode="day", ds="2026-09-13")
    generate_for_day("2026-09-13", 5000)   # 日更入口：造数 + 幂等写回
"""
import decimal
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import pandas as pd

# generator/ 在项目根下，把项目根加进 sys.path，shared 包才能被 import
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.log import setup_logging, get_logger   # noqa: E402  (path 处理后才能 import)

setup_logging()   # 本文件也是独立入口（__main__），自己负责 setup
logger = get_logger(__name__)

from config import settings   # noqa: E402
from build_dimensions import (   # noqa: E402
    DimStatus,
    read_parquet,
    s3_client,
    write_parquet,
    load_customer,
    load_account,
    load_merchant,
)
from schema import Currency, Transaction, TransactionType   # noqa: E402

# 数据生成模式：batch=批量补历史 / realtime=实时流 / day=日更造当天
GenMode = Literal["batch", "realtime", "day"]

# 交易事实表在 fact 桶里的对象名
KEY_TRANSACTION = "fact_transaction.parquet"


def load_dimension_pools() -> dict:
    """从 MinIO 读维度，只收 ACTIVE 的客户/商户（软删实体不参与新交易）。"""
    client = s3_client()
    cust_df = load_customer(client)
    acc_df = load_account(client)
    merc_df = load_merchant(client)

    active_customers = cust_df.loc[
        cust_df["status"] == DimStatus.ACTIVE.value, "customer_id"
    ].tolist()
    active_merchants = merc_df.loc[
        merc_df["status"] == DimStatus.ACTIVE.value, "merchant_id"
    ].tolist()
    active_customer_set = set(active_customers)

    # 客户 → {账户: 开户时间}，只保留 ACTIVE 客户名下的账户
    accounts_by_customer: dict[str, dict[str, datetime]] = {}
    for cid, aid, open_time in zip(
        acc_df["customer_id"], acc_df["account_id"], acc_df["open_time"]
    ):
        if cid not in active_customer_set:
            continue
        accounts_by_customer.setdefault(cid, {})[aid] = open_time

    assert active_customers and active_merchants, "维度池为空（无 ACTIVE 客户/商户），先造维度"
    return {
        "customers": active_customers,
        "merchants": active_merchants,
        "accounts_by_customer": accounts_by_customer,
    }


def make_transaction(pools: dict, mode: GenMode = "realtime", ds: str | None = None) -> Transaction:
    """随机造一条合法交易，ID 全部来自 ACTIVE 维度池。

    mode:
        batch    批量：event_time = 开户时间 ~ 当前时间之间的随机时刻（补历史数据）
        realtime 实时：event_time = 当前时间往前 0~5 秒（制造乱序，给 Flink Watermark）
        day      日更：event_time 均匀落在 ds 当天 00:00:00~23:59:59（UTC）
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
    elif mode == "day":
        # 日更：ds 当天均匀分布（UTC）
        assert ds, "day 模式必须传 ds（如 2026-09-13）"
        day_start = datetime.fromisoformat(ds).replace(tzinfo=timezone.utc)
        event_time = day_start + timedelta(seconds=random.uniform(0, 86399.999))
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
        # 用 random 而非 uuid4：uuid4 走 os.urandom 不受 seed 控制，
        # 日更模式 seed(ds) 下重跑会换一批 ID；getrandbits 同种子结果一致（格式仍是 T+16 位 hex）
        transaction_id=f"T{random.getrandbits(64):016x}",
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


def make_day_transactions(pools: dict, ds: str, count: int) -> list[Transaction]:
    """日更造 count 条：event_time 全部落在 ds 当天（调用前需先 seed(ds) 保证可复现）。"""
    return [make_transaction(pools, mode="day", ds=ds) for _ in range(count)]


def make_realtime_transaction(pools: dict) -> Transaction:
    """实时模式造一条：event_time = 当前时间往前 0~5 秒（实时流 + 乱序）。"""
    return make_transaction(pools, mode="realtime")


def save_transactions_to_minio(txns: list[Transaction], override: bool = False) -> None:
    """交易流水写成 Parquet 上传 MinIO；默认追加到旧数据后面，override=True 覆盖重来。"""
    client = s3_client()
    df_new = pd.DataFrame([t.model_dump() for t in txns])   # 模型 → dict → DataFrame

    # put_object 是覆盖写，追加 = 读旧数据 + concat + 写回
    old = read_parquet(client, KEY_TRANSACTION, bucket=settings.minio_bucket_fact)
    df = df_new if old is None or override else pd.concat([old, df_new], axis=0, ignore_index=True)

    write_parquet(client, df, KEY_TRANSACTION, bucket=settings.minio_bucket_fact)
    logger.info(
        "写入 MinIO 完成",
        mode="覆盖" if override else "追加",
        bucket=settings.minio_bucket_fact,
        key=KEY_TRANSACTION,
        new_rows=len(df_new),
        total_rows=len(df),
    )


def save_day_transactions_to_minio(txns: list[Transaction], ds: str) -> None:
    """日更幂等写回：先剔除旧数据里 event_time 属于 ds 的行，再 concat 新行整体写回。

    这样 Airflow 重试 / 手动重跑同一天不会重复造数据，
    下游 ODS 的 to_date(event_time) = ds 过滤口径也零改动。
    """
    client = s3_client()
    df_new = pd.DataFrame([t.model_dump() for t in txns])
    target_day = datetime.fromisoformat(ds).date()

    old = read_parquet(client, KEY_TRANSACTION, bucket=settings.minio_bucket_fact)
    if old is None:
        df = df_new
    else:
        old_dt = pd.to_datetime(old["event_time"], utc=True)
        kept = old[old_dt.dt.date != target_day]
        dropped = len(old) - len(kept)
        df = pd.concat([kept, df_new], axis=0, ignore_index=True)

    write_parquet(client, df, KEY_TRANSACTION, bucket=settings.minio_bucket_fact)
    logger.info(
        "当日流水幂等写回完成",
        ds=ds,
        dropped_same_day_rows=dropped if old is not None else 0,
        new_rows=len(df_new),
        total_rows=len(df),
    )


def generate_for_day(ds: str, count: int) -> None:
    """日更总入口（daily_txn.py 调这个）：seed(ds) → ACTIVE 池造当天流水 → 幂等写回。"""
    random.seed(ds)                       # 与维度演进同一个种子规则：同 ds 结果可复现
    pools = load_dimension_pools()
    txns = make_day_transactions(pools, ds, count)
    save_day_transactions_to_minio(txns, ds)


if __name__ == "__main__":
    pools = load_dimension_pools()
    txns = make_batch_transaction(pools, count=90000)
    save_transactions_to_minio(txns)
