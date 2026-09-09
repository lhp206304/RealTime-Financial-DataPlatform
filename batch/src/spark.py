"""SparkSession 工厂：统一配 MinIO S3A + ClickHouse + Python worker。

所有 pipelines 表任务共用这一个工厂，改配置只改这里。
注意：共用的是工厂函数；每个表任务是独立进程，各自 getOrCreate() 出自己的 session。
"""
import os
import sys

from pyspark.sql import SparkSession

from config.settings import settings

# ---- Python worker：用容器镜像自带的 Python（Spark 镜像已配好）----
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def get_spark_session(app_name: str = "batch-pipeline") -> SparkSession:
    """创建配好 MinIO + ClickHouse 的 SparkSession。"""
    return (
        SparkSession.builder
        .master(settings.spark_master)
        .appName(app_name)
        # ---- Python worker：用 venv 的解释器，防止版本不匹配 ----
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        # ---- MinIO S3A 连接 ----
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # ---- ClickHouse + S3A 依赖 jar 由 Dockerfile 装进 /opt/spark/jars/，spark-submit 自动加载 ----
        # 注册名为 clickhouse 的 catalog：之后用 clickhouse.finance.{table} 三段名访问
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", settings.clickhouse_host)
        .config("spark.sql.catalog.clickhouse.protocol", "http")
        .config("spark.sql.catalog.clickhouse.http_port", str(settings.clickhouse_http_port))
        .config("spark.sql.catalog.clickhouse.user", settings.clickhouse_user)
        .config("spark.sql.catalog.clickhouse.password", settings.clickhouse_password)
        .config("spark.sql.catalog.clickhouse.database", settings.clickhouse_database)
        .getOrCreate()
    )
