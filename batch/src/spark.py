"""SparkSession 工厂：统一配 MinIO S3A + ClickHouse + Python worker。

所有 pipelines 表任务共用这一个工厂，改配置只改这里。
注意：共用的是工厂函数；每个表任务是独立进程，各自 getOrCreate() 出自己的 session。
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from config.settings import settings

# ---- Java 版本：pyspark 4.x 要求 Java 17+，本机默认 java 是 11，这里强制指定 ----
# 只影响 batch 进程（JVM 子进程继承），不改动全局 JAVA_HOME
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# ---- Python worker：环境变量优先级最高，保证 executor worker 也用 venv 的 3.14 ----
# （只配 spark.pyspark.python 时 executor 侧可能回落到系统 python3，遇到新语法直接崩）
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ---- ClickHouse Spark Connector jar（本地 batch/jars/ 下）----
# runtime 0.10.0 兼容 Spark 4.0+（pyspark 4.2 可用）；jdbc 0.9.5-all 是其传输依赖
CH_JARS_DIR = Path(__file__).resolve().parents[1] / "jars"

def get_spark_session(app_name: str = "batch-pipeline") -> SparkSession:
    """创建配好 MinIO + ClickHouse 的 SparkSession。"""
    return (
        SparkSession.builder
        .master("local[*]")
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
        # ---- MinIO S3 依赖 jar（跑时自动下载）----
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.3,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.782")
        # ---- ClickHouse Spark Connector（分布式读写，不再 toPandas 单机拉数据）----
        # 多个 jar 必须逗号拼在同一行：.config 同 key 后写覆盖前写，拆两行会只剩最后一个
        .config("spark.jars", (f"{CH_JARS_DIR}/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar,"
                               f"{CH_JARS_DIR}/clickhouse-jdbc-0.9.5-all.jar"))
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
