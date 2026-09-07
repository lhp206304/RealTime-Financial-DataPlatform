"""维度表：生成三张维表，写入 / 读取 MinIO（Parquet 格式）。

流程：
    1. build_* 在内存里造出维度数据（客户/账户/商户）
    2. save_dimensions_to_minio 把它们写成 Parquet 传到 MinIO
    3. load_dimensions_from_minio 从 MinIO 读回来（造交易时用）

约定：account 绑定 customer（一个客户 1~5 个账户），交易里的 ID
都要能在这三张维表里查到，保证下游 JOIN 不落空。
"""
import io
import random
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

from schema import (
    AccountType,
    CustomerLevel,
    DimAccount,
    DimCustomer,
    DimMerchant,
    RiskLevel,
)



# ---- MinIO 连接（本机跑，连 localhost:9000）----
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "dim"                # 维度表所在的桶

# 三张维表在桶里的对象名（Parquet 文件）
KEY_CUSTOMER = "dim_customer.parquet"
KEY_ACCOUNT = "dim_account.parquet"
KEY_MERCHANT = "dim_merchant.parquet"

_REGIONS = ["北京", "上海", "广州", "深圳", "杭州", "成都"]
_CATEGORIES = ["餐饮", "零售", "电商", "出行", "娱乐", "教育", "医疗"]



def s3_client()->boto3.client.S3.Client:
    """建一个连 MinIO 的 S3 客户端（MinIO 兼容 S3 协议）。"""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

# ---- 写 / 读 MinIO ----
def read_parquet(client, key: str, bucket: str = MINIO_BUCKET) -> pd.DataFrame | None:
    """从 MinIO 读一个 Parquet 回 DataFrame；文件不存在返回 None。"""
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read())) if obj else None
    except:
        return None


def write_parquet(client, df: pd.DataFrame, key: str, bucket: str = MINIO_BUCKET) -> None:
    """把 DataFrame 写入 MinIO（put_object 是覆盖写）。"""
    # 没有 bucket 就创建
    existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)         # 写进内存缓冲
    buf.seek(0)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

def load_customer(client: boto3.client.S3.Client)->pd.DataFrame|None:
    """从 MinIO 读一个 Parquet 回 DataFrame。"""
    return read_parquet(client, KEY_CUSTOMER)

def load_account(client: boto3.client.S3.Client)->pd.DataFrame|None:
    """从 MinIO 读一个 Parquet 回 DataFrame。"""
    return read_parquet(client, KEY_ACCOUNT)
    
def load_merchant(client: boto3.client.S3.Client)->pd.DataFrame|None:
    """从 MinIO 读一个 Parquet 回 DataFrame。"""
    return read_parquet(client, KEY_MERCHANT)
    
def add_customer_account(num:int,num_of_days:int,override:bool=False)->None:
    """造出 num 个客户，注册时间在最近 num_of_days 天前的近30天内。"""
    #找出最大的客户ID
    client=s3_client()
    customer_df=load_customer(client)
    account_df = load_account(client)
    if account_df is None or override :
        max_account_id=10000
    else:
        max_account_id=account_df["account_id"].str[1:].astype(int).max()
    if customer_df is  None or override:
        max_customer_id=10000
    else:
        max_customer_id=customer_df["customer_id"].str[1:].astype(int).max()

    
    customers: list[DimCustomer]=[]
    accounts: list[DimAccount]=[]
    now=datetime.now(timezone.utc)
    lists=list(CustomerLevel)
    for i in range(1,num+1):
        cust_id=f"C{max_customer_id+1}"
        cust_register_time=now - timedelta(
            days=random.randint(num_of_days, num_of_days+30),
            hours=random.randint(0,4),
            minutes=random.randint(0,59),
            )
        customers.append(
            DimCustomer(
                customer_id=cust_id,
                level=random.choice(lists),
                region=random.choice(_REGIONS),
                register_time=cust_register_time,
            )
        )
        max_customer_id+=1
        # 这个客户绑 1~5 个账户
        for _ in range(random.randint(1, 5)):
            accounts.append(
                DimAccount(
                    account_id=f"A{max_account_id+1}",
                    customer_id=cust_id,
                    account_type=random.choice(list(AccountType)),
                    open_time=cust_register_time+timedelta(
                        hours=random.uniform(1,240),
                        ),
                )
            )
            max_account_id+=1
    customer_df_new=pd.DataFrame([c.model_dump() for c in customers])
    account_df_new=pd.DataFrame([a.model_dump() for a in accounts])
    if customer_df is None or override:
        customer_df=customer_df_new
        account_df=account_df_new
    else:
        customer_df=pd.concat([customer_df,customer_df_new],axis=0,ignore_index=True)
        account_df=pd.concat([account_df,account_df_new],axis=0,ignore_index=True)
    write_parquet(client, customer_df, KEY_CUSTOMER)
    write_parquet(client, account_df, KEY_ACCOUNT)
    print(f"【{"覆盖" if override else "新增"}】写入MinIO【{num}】个客户和每个客户随机1-5个账户")

def add_merchant(num:int,override:bool=False)->None:
    """造出 num 个商户。"""
    client=s3_client()
    merchant_df=load_merchant(client)
    if merchant_df is None or override:
        max_merchant_id=10000
    else:
        max_merchant_id=merchant_df["merchant_id"].str[1:].astype(int).max()
    merchants: list[DimMerchant]=[]
    for i in range(1,num+1):
        merchants.append(
            DimMerchant(
                merchant_id=f"M{max_merchant_id+1}",
                category=random.choice(_CATEGORIES),
                region=random.choice(_REGIONS),
                risk_level=random.choice(list(RiskLevel)),
            )
        )
        max_merchant_id+=1
    merchant_df_new=pd.DataFrame([m.model_dump() for m in merchants])
    if merchant_df is None or override:
        merchant_df=merchant_df_new
    else:
        merchant_df=pd.concat([merchant_df,merchant_df_new],axis=0,ignore_index=True)
    write_parquet(client, merchant_df, KEY_MERCHANT)
    print(f"【{"覆盖" if override else "新增"}】写入MinIO【{num}】个商户")


if __name__ == "__main__":
    # 直接跑这个文件 = 造维度并落地 MinIO（V2 第一步）
    add_customer_account(10000,30,True )
    add_merchant(100,True )
