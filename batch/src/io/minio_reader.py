"""读 MinIO 原始数据：事实表（fact 桶）+ 维度表（dim 桶）。

数据由 generator/ 造好写入 MinIO（batch 只负责读、不造数）：
    generator/build_dimensions.py   → dim 桶：dim_customer.parquet / dim_merchant.parquet
    generator/build_transactions.py → fact 桶：fact_transaction.parquet
"""
from pyspark.sql import DataFrame, SparkSession

from config.settings import settings

# MinIO 桶类型
_BUCK_TYPE={
    "fact":settings.minio_bucket_fact,
    "dim":settings.minio_bucket_dim,
}
# 参数含义：
#     spark：SparkSession 实例
#     bucket_type：fact 桶或 dim 桶
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

