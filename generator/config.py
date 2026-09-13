"""generator 连接配置：全部从环境变量读，无默认值。

变量命名与 batch / api / docker-compose 完全一致（MINIO_*）：
    本机跑：连 localhost:9000；容器内跑：连 minio:9000，由 deploy/.env 决定。
缺失任何一个变量，BaseSettings 在 import 时直接报错，拒绝带默认值启动。
"""
from pydantic_settings import BaseSettings


class GeneratorSettings(BaseSettings):
    # 无前缀：字段名 minio_endpoint 自动读环境变量 MINIO_ENDPOINT
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket_dim: str       # 维度桶：dim_customer/dim_account/dim_merchant.parquet
    minio_bucket_fact: str      # 事实桶：fact_transaction.parquet


settings = GeneratorSettings()
