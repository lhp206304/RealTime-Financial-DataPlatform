"""连接参数：所有 MinIO / StarRocks / Redis 地址集中在这。

环境变量覆盖默认值（前缀 BATCH_）：
    BATCH_MINIO_ENDPOINT=http://minio:9000  → 覆盖 localhost
    BATCH_STARROCKS_HOST=starrocks          → 覆盖 localhost
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ---- MinIO（S3 兼容，原始数据由 generator/ 写入，batch 只读）----
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket_dim: str = "dim"      # 维度表桶：dim_customer.parquet / dim_merchant.parquet
    minio_bucket_fact: str = "fact"    # 事实表桶：fact_transaction.parquet（历史交易）

    # ---- StarRocks（OLAP 汇聚层）----
    starrocks_host: str = "localhost"
    starrocks_port: int = 8030          # FE HTTP 端口（Spark Connector Stream Load 用）
    starrocks_query_port: int = 9030    # FE MySQL 协议端口（pymysql 执行 DDL/DELETE 用）
    starrocks_user: str = "root"
    starrocks_password: str = ""
    starrocks_database: str = "finance"

    # ---- Redis（维表缓存，实时链路用）----
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    model_config = {"env_prefix": "BATCH_"}


# 单例：import 一次到处用
settings = Settings()
