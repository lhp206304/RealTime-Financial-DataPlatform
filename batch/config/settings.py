"""连接参数：所有 MinIO / ClickHouse / Redis 地址集中在这。

默认值直接用 Docker 服务名（minio/clickhouse/redis），
容器内直接连通，无需 compose environment 注入。
env_prefix 保留：以后换环境（如 K8s）可用环境变量覆盖。
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ---- MinIO（S3 兼容，原始数据由 generator/ 写入，batch 只读）----
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket_dim: str = "dim"      # 维度表桶：dim_customer.parquet / dim_merchant.parquet
    minio_bucket_fact: str = "fact"    # 事实表桶：fact_transaction.parquet（历史交易）

    # ---- ClickHouse（离线 OLAP 汇聚层，PySpark T+1 写入）----
    clickhouse_host: str = "clickhouse"
    clickhouse_http_port: int = 8123        # HTTP 端口（clickhouse-connect / Spark Catalog 用）
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "finance"

    # ---- Redis（维表缓存，实时链路用）----
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0

    # ---- Spark ----
    spark_master: str = "spark://spark-master:7077"

    model_config = {"env_prefix": "BATCH_"}

# 单例：import 一次到处用
settings = Settings()
