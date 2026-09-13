"""维度表：生成/演进三张维表，写入 / 读取 MinIO（Parquet 格式）。

两类入口：
    1. 首次铺底：__main__ 直接跑（add_customer_account + add_merchant，override=True）
    2. 每日演进：evolve_for_day(ds, ...) —— 新增 / 更新 / 软删 customer、merchant
       演进前自动做当日快照（history/dt={ds}/），重跑同一天先恢复快照，保证可复现

软删除约定：
    不物理删行，只把 status 置为 DELETED；造交易池只取 ACTIVE。
    account 不做软删（随客户保留历史）。
"""
import io
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd

# generator/ 在项目根下，把项目根加进 sys.path，shared 包才能被 import
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.log import setup_logging, get_logger   # noqa: E402  (path 处理后才能 import)

setup_logging()   # 本文件也是独立入口（__main__），自己负责 setup
logger = get_logger(__name__)

from config import settings   # noqa: E402
from schema import (   # noqa: E402
    AccountType,
    CustomerLevel,
    DimAccount,
    DimCustomer,
    DimMerchant,
    DimStatus,
    RiskLevel,
)

# 三张维表在 dim 桶里的对象名（Parquet 文件）
KEY_CUSTOMER = "dim_customer.parquet"
KEY_ACCOUNT = "dim_account.parquet"
KEY_MERCHANT = "dim_merchant.parquet"
_DIM_KEYS = (KEY_CUSTOMER, KEY_ACCOUNT, KEY_MERCHANT)

# 每日演进前的快照前缀（同在 dim 桶）：history/dt=2026-09-13/dim_customer.parquet
_SNAPSHOT_PREFIX = "history/dt={ds}/"

_REGIONS = ["北京", "上海", "广州", "深圳", "杭州", "成都"]
_CATEGORIES = ["餐饮", "零售", "电商", "出行", "娱乐", "教育", "医疗"]


def s3_client():
    """建一个连 MinIO 的 S3 客户端（地址/密钥全部来自环境变量）。

    不写返回类型：boto3 没有公开的 Client 类型，旧写法 boto3.client.S3.Client
    在新版 boto3 上直接 AttributeError（boto3.client 是函数，不是模块）。
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )


# ---- 写 / 读 MinIO ----
def read_parquet(client, key: str, bucket: str = settings.minio_bucket_dim) -> pd.DataFrame | None:
    """从 MinIO 读一个 Parquet 回 DataFrame；文件不存在返回 None。"""
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read())) if obj else None
    except Exception:
        return None


def write_parquet(client, df: pd.DataFrame, key: str, bucket: str = settings.minio_bucket_dim) -> None:
    """把 DataFrame 写入 MinIO（put_object 是覆盖写）。"""
    # 没有 bucket 就创建
    existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)         # 写进内存缓冲
    buf.seek(0)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def _with_status(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """旧 parquet 没有 status 列（软删功能上线前造的数据）→ 补 ACTIVE。"""
    if df is not None and "status" not in df.columns:
        df = df.copy()
        df["status"] = DimStatus.ACTIVE.value
    return df


def load_customer(client) -> pd.DataFrame | None:
    """从 MinIO 读客户维表，自动补 status 列。"""
    return _with_status(read_parquet(client, KEY_CUSTOMER))


def load_account(client) -> pd.DataFrame | None:
    """从 MinIO 读账户维表。"""
    return read_parquet(client, KEY_ACCOUNT)


def load_merchant(client) -> pd.DataFrame | None:
    """从 MinIO 读商户维表，自动补 status 列。"""
    return _with_status(read_parquet(client, KEY_MERCHANT))


# ---- 首次铺底：客户/账户 ----
def add_customer_account(num: int, num_of_days: int, override: bool = False) -> None:
    """造出 num 个客户，注册时间在最近 num_of_days 天前的近30天内。"""
    # 找出最大的客户 ID，新 ID 在其基础上递增
    client = s3_client()
    customer_df = load_customer(client)
    account_df = load_account(client)
    if account_df is None or override:
        max_account_id = 10000
    else:
        max_account_id = account_df["account_id"].str[1:].astype(int).max()
    if customer_df is None or override:
        max_customer_id = 10000
    else:
        max_customer_id = customer_df["customer_id"].str[1:].astype(int).max()

    customers: list[DimCustomer] = []
    accounts: list[DimAccount] = []
    now = datetime.now(timezone.utc)
    lists = list(CustomerLevel)
    for i in range(1, num + 1):
        cust_id = f"C{max_customer_id + 1}"
        cust_register_time = now - timedelta(
            days=random.randint(num_of_days, num_of_days + 30),
            hours=random.randint(0, 4),
            minutes=random.randint(0, 59),
        )
        customers.append(
            DimCustomer(
                customer_id=cust_id,
                level=random.choice(lists),
                region=random.choice(_REGIONS),
                register_time=cust_register_time,
            )
        )
        max_customer_id += 1
        # 这个客户绑 1~5 个账户
        for _ in range(random.randint(1, 5)):
            accounts.append(
                DimAccount(
                    account_id=f"A{max_account_id + 1}",
                    customer_id=cust_id,
                    account_type=random.choice(list(AccountType)),
                    open_time=cust_register_time + timedelta(
                        hours=random.uniform(1, 240),
                    ),
                )
            )
            max_account_id += 1
    customer_df_new = pd.DataFrame([c.model_dump() for c in customers])
    account_df_new = pd.DataFrame([a.model_dump() for a in accounts])
    if customer_df is None or override:
        customer_df = customer_df_new
        account_df = account_df_new
    else:
        customer_df = pd.concat([customer_df, customer_df_new], axis=0, ignore_index=True)
        account_df = pd.concat([account_df, account_df_new], axis=0, ignore_index=True)
    write_parquet(client, customer_df, KEY_CUSTOMER)
    write_parquet(client, account_df, KEY_ACCOUNT)
    logger.info(
        "客户/账户写入 MinIO 完成",
        mode="覆盖" if override else "新增",
        customers=num,
    )


# ---- 每日新增：客户注册时间落在 ds 当天凌晨，保证当天交易晚于开户 ----
def add_customers(num: int, ds: str) -> None:
    """每日演进用：新增 num 个客户（含 1~5 个账户），注册/开户时间都在 ds 当天。"""
    client = s3_client()
    customer_df = load_customer(client)
    account_df = load_account(client)
    if customer_df is None or account_df is None:
        raise RuntimeError("维表不存在，请先跑铺底：python build_dimensions.py")

    max_customer_id = customer_df["customer_id"].str[1:].astype(int).max()
    max_account_id = account_df["account_id"].str[1:].astype(int).max()
    day_start = datetime.fromisoformat(ds).replace(tzinfo=timezone.utc)

    customers: list[DimCustomer] = []
    accounts: list[DimAccount] = []
    for _ in range(num):
        # 注册时间：当天 00:00 后的 0~5 分钟内；开户：注册后 1~60 分钟 → 必在当天交易之前
        register_time = day_start + timedelta(seconds=random.randint(0, 300))
        cust_id = f"C{max_customer_id + 1}"
        customers.append(
            DimCustomer(
                customer_id=cust_id,
                level=random.choice(list(CustomerLevel)),
                region=random.choice(_REGIONS),
                register_time=register_time,
            )
        )
        max_customer_id += 1
        for _ in range(random.randint(1, 5)):
            accounts.append(
                DimAccount(
                    account_id=f"A{max_account_id + 1}",
                    customer_id=cust_id,
                    account_type=random.choice(list(AccountType)),
                    open_time=register_time + timedelta(seconds=random.randint(60, 3600)),
                )
            )
            max_account_id += 1

    customer_df = pd.concat(
        [customer_df, pd.DataFrame([c.model_dump() for c in customers])],
        axis=0, ignore_index=True,
    )
    account_df = pd.concat(
        [account_df, pd.DataFrame([a.model_dump() for a in accounts])],
        axis=0, ignore_index=True,
    )
    write_parquet(client, customer_df, KEY_CUSTOMER)
    write_parquet(client, account_df, KEY_ACCOUNT)
    logger.info("每日新增客户完成", ds=ds, customers=num, accounts=len(accounts))


# ---- 首次铺底 / 每日新增：商户 ----
def add_merchant(num: int, override: bool = False) -> None:
    """造出 num 个商户（新商户 status 默认 ACTIVE）。"""
    client = s3_client()
    merchant_df = load_merchant(client)
    if merchant_df is None or override:
        max_merchant_id = 10000
    else:
        max_merchant_id = merchant_df["merchant_id"].str[1:].astype(int).max()
    merchants: list[DimMerchant] = []
    for i in range(1, num + 1):
        merchants.append(
            DimMerchant(
                merchant_id=f"M{max_merchant_id + 1}",
                category=random.choice(_CATEGORIES),
                region=random.choice(_REGIONS),
                risk_level=random.choice(list(RiskLevel)),
            )
        )
        max_merchant_id += 1
    merchant_df_new = pd.DataFrame([m.model_dump() for m in merchants])
    if merchant_df is None or override:
        merchant_df = merchant_df_new
    else:
        merchant_df = pd.concat([merchant_df, merchant_df_new], axis=0, ignore_index=True)
    write_parquet(client, merchant_df, KEY_MERCHANT)
    logger.info(
        "商户写入 MinIO 完成",
        mode="覆盖" if override else "新增",
        merchants=num,
    )


# ---- 每日更新 / 软删 ----
def _pick_active_ids(df: pd.DataFrame, id_col: str, n: int) -> list[str]:
    """从 ACTIVE 行里随机挑 n 个 ID；ACTIVE 不够直接报错（暴露配置问题）。"""
    active = df.loc[df["status"] == DimStatus.ACTIVE.value, id_col].tolist()
    assert len(active) >= n, f"ACTIVE {id_col} 只有 {len(active)} 个，不够挑 {n} 个"
    random.shuffle(active)
    return active[:n]


def update_customers(n: int) -> None:
    """随机挑 n 个 ACTIVE 客户改 level / region（ID、注册时间、status 不动）。"""
    client = s3_client()
    df = load_customer(client)
    ids = _pick_active_ids(df, "customer_id", n)
    id_set = set(ids)
    for cust_id in ids:
        cur = df.loc[df["customer_id"] == cust_id, "level"].iloc[0]
        # 等级强制换成另一个，保证"更新"确实改了值
        new_level = random.choice([l for l in CustomerLevel if l.value != str(cur)])
        df.loc[df["customer_id"] == cust_id, "level"] = new_level.value
        df.loc[df["customer_id"] == cust_id, "region"] = random.choice(_REGIONS)
    write_parquet(client, df, KEY_CUSTOMER)
    logger.info("每日更新客户完成", customers=n, sample=list(id_set)[:5])


def soft_delete_customers(n: int) -> None:
    """随机挑 n 个 ACTIVE 客户置 DELETED（不删行、不动账户）。"""
    client = s3_client()
    df = load_customer(client)
    ids = set(_pick_active_ids(df, "customer_id", n))
    df.loc[df["customer_id"].isin(ids), "status"] = DimStatus.DELETED.value
    write_parquet(client, df, KEY_CUSTOMER)
    logger.info("每日软删客户完成", customers=n, sample=list(ids)[:5])


def update_merchants(n: int) -> None:
    """随机挑 n 个 ACTIVE 商户改 category / region / risk_level。"""
    client = s3_client()
    df = load_merchant(client)
    ids = _pick_active_ids(df, "merchant_id", n)
    id_set = set(ids)
    for merc_id in ids:
        df.loc[df["merchant_id"] == merc_id, "category"] = random.choice(_CATEGORIES)
        df.loc[df["merchant_id"] == merc_id, "region"] = random.choice(_REGIONS)
        df.loc[df["merchant_id"] == merc_id, "risk_level"] = random.choice(list(RiskLevel)).value
    write_parquet(client, df, KEY_MERCHANT)
    logger.info("每日更新商户完成", merchants=n, sample=list(id_set)[:5])


def soft_delete_merchants(n: int) -> None:
    """随机挑 n 个 ACTIVE 商户置 DELETED。"""
    client = s3_client()
    df = load_merchant(client)
    ids = set(_pick_active_ids(df, "merchant_id", n))
    df.loc[df["merchant_id"].isin(ids), "status"] = DimStatus.DELETED.value
    write_parquet(client, df, KEY_MERCHANT)
    logger.info("每日软删商户完成", merchants=n, sample=list(ids)[:5])


# ---- 快照 / 恢复（每日演进幂等的基础）----
def _snapshot_prefix(ds: str) -> str:
    return _SNAPSHOT_PREFIX.format(ds=ds)


def _object_exists(client, key: str) -> bool:
    try:
        client.head_object(Bucket=settings.minio_bucket_dim, Key=key)
        return True
    except Exception:
        return False


def snapshot_exists(client, ds: str) -> bool:
    """当日快照是否已存在（以客户表为标记文件）。"""
    return _object_exists(client, f"{_snapshot_prefix(ds)}{KEY_CUSTOMER}")


def list_snapshot_days(client) -> list[str]:
    """列出所有快照日期（history/ 下的 dt= 前缀），ISO 日期可直接字符串比较。"""
    days: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": settings.minio_bucket_dim, "Prefix": "history/", "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        days += [p["Prefix"].split("dt=")[1].rstrip("/") for p in resp.get("CommonPrefixes", [])]
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return sorted(days)


def snapshot_dimensions(client, ds: str) -> None:
    """演进前把当前三张维表复制到 history/dt={ds}/（只复制存在的）。"""
    for key in _DIM_KEYS:
        if _object_exists(client, key):
            client.copy_object(
                Bucket=settings.minio_bucket_dim,
                Key=f"{_snapshot_prefix(ds)}{key}",
                CopySource={"Bucket": settings.minio_bucket_dim, "Key": key},
            )
    logger.info("维度快照完成", ds=ds)


def restore_snapshot(client, ds: str) -> None:
    """把 history/dt={ds}/ 的三张表复制回正式位置（重跑前回滚）。"""
    for key in _DIM_KEYS:
        snap_key = f"{_snapshot_prefix(ds)}{key}"
        if _object_exists(client, snap_key):
            client.copy_object(
                Bucket=settings.minio_bucket_dim,
                Key=key,
                CopySource={"Bucket": settings.minio_bucket_dim, "Key": snap_key},
            )
    logger.info("维度快照恢复完成", ds=ds)


# ---- 每日演进总入口（daily_evolve.py 调这个）----
def evolve_for_day(
    ds: str,
    *,
    n_add_customer: int,
    n_update_customer: int,
    n_delete_customer: int,
    n_add_merchant: int,
    n_update_merchant: int,
    n_delete_merchant: int,
) -> None:
    """按 ds 做一天的维度演进。随机种子固定为 ds，配合快照实现幂等重跑。"""
    # 日期格式先校验，fail fast
    datetime.fromisoformat(ds)

    client = s3_client()
    random.seed(ds)

    if snapshot_exists(client, ds):
        # 重跑同一天：先回滚到当日演进前的状态，再用同样的种子重演
        logger.info("发现当日快照，恢复后重新演进", ds=ds)
        restore_snapshot(client, ds)
    else:
        # 回填保护：已存在比 ds 更新的快照时拒绝执行，避免冲掉后续几天的演进
        newer = [d for d in list_snapshot_days(client) if d > ds]
        assert not newer, f"已存在更新日期的快照 {newer}，禁止回填 {ds}"
        snapshot_dimensions(client, ds)

    add_customers(n_add_customer, ds)
    update_customers(n_update_customer)
    soft_delete_customers(n_delete_customer)
    add_merchant(n_add_merchant)
    update_merchants(n_update_merchant)
    soft_delete_merchants(n_delete_merchant)
    logger.info(
        "维度每日演进完成",
        ds=ds,
        customer_added=n_add_customer,
        customer_updated=n_update_customer,
        customer_deleted=n_delete_customer,
        merchant_added=n_add_merchant,
        merchant_updated=n_update_merchant,
        merchant_deleted=n_delete_merchant,
    )


if __name__ == "__main__":
    # 直接跑这个文件 = 首次铺底（全量覆盖造维度并落地 MinIO）
    add_customer_account(10000, 30, True)
    add_merchant(100, True)
