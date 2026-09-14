"""读 MinIO 原始数据：主数据（master 桶）+ 交易流水（transaction 桶）。

MinIO 是数仓上游的数据湖，桶按「上游数据语义」命名（主数据 / 业务流水），
dim / fact 是下游数仓的建模概念，不出现在上游桶名里。
数据由 generator/ 造好写入 MinIO（batch 只负责读、不造数）：
    generator/build_dimensions.py   → master 桶：dim_customer.parquet / dim_merchant.parquet
    generator/build_transactions.py → transaction 桶：fact_transaction.parquet
"""
from pyspark.sql import DataFrame, SparkSession

from config.settings import settings

# MinIO 桶类型（按上游数据语义：master=主数据，transaction=交易流水）
_BUCK_TYPE={
    "transaction": settings.minio_bucket_transaction,
    "master": settings.minio_bucket_master,
}
# 参数含义：
#     spark：SparkSession 实例
#     bucket_type：master 桶（主数据）或 transaction 桶（交易流水）
#     table_name：表名（不包含 .parquet）
#     dt：日期，如 "2023-01-01"，若 None 则读全表
def read_minio(spark:SparkSession,bucket_type:str,table_name:str,dt:str|None=None) ->DataFrame:
    if bucket_type not in _BUCK_TYPE:
        raise ValueError(f"未知桶类型 {bucket_type!r}，可选：{list(_BUCK_TYPE)}")
    path=f"s3a://{_BUCK_TYPE[bucket_type]}/{table_name}.parquet"
    df=spark.read.parquet(path)
    if dt:
        df=df.filter(f"to_date(event_time) = '{dt}'")
    return df
