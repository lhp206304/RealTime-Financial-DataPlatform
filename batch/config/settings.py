"""连接参数：MinIO / ClickHouse / Redis / Spark 地址集中在这。

变量命名按「基础设施资源」（MINIO_* / CLICKHOUSE_* ...），全项目统一：
docker-compose 的 environment 注入同名变量，api 模块也用同一套名字。
batch 只在容器内跑，连接信息无默认值——环境变量缺失时 BaseSettings 启动直接报错，
强制 deploy/.env 提供真实值。
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ---- MinIO（S3 兼容，原始数据由 generator/ 写入，batch 只读）----
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket_master: str       # 主数据桶：dim_customer.parquet / dim_merchant.parquet
    minio_bucket_transaction: str  # 交易流水桶：fact_transaction.parquet（历史交易）

    # ---- ClickHouse（离线 OLAP 汇聚层，PySpark T+1 写入）----
    clickhouse_host: str
    clickhouse_http_port: int        # HTTP 端口（clickhouse-connect / Spark Catalog 用）
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str

    # ---- Redis（维表缓存，实时链路用）----
    redis_host: str
    redis_port: int
    redis_db: int

    # ---- Spark ----
    spark_master: str


# 单例：import 一次到处用。
# 无前缀：字段名 minio_endpoint 自动读环境变量 MINIO_ENDPOINT（与 api/compose 同名）。
settings = Settings()
