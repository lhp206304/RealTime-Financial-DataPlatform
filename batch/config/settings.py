"""连接参数：所有 MinIO / ClickHouse / Redis 地址集中在这。

环境变量覆盖默认值（前缀 BATCH_）：
    BATCH_MINIO_ENDPOINT=http://minio:9000      → 覆盖 localhost
    BATCH_CLICKHOUSE_HOST=clickhouse            → 覆盖 localhost
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ---- MinIO（S3 兼容，原始数据由 generator/ 写入，batch 只读）----
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket_dim: str = "dim"      # 维度表桶：dim_customer.parquet / dim_merchant.parquet
    minio_bucket_fact: str = "fact"    # 事实表桶：fact_transaction.parquet（历史交易）

    # ---- ClickHouse（离线 OLAP 汇聚层，PySpark T+1 写入）----
    clickhouse_host: str = "localhost"
    clickhouse_http_port: int = 8123        # HTTP 端口（clickhouse-connect / Spark Catalog 用）
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "finance"

    # ---- Redis（维表缓存，实时链路用）----
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    model_config = {"env_prefix": "BATCH_"}

# 单例：import 一次到处用
settings = Settings()
