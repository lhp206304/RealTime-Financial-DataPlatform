"""SparkSession 工厂：统一配 MinIO S3A + StarRocks + Python worker。

所有 pipelines 表任务共用这一个工厂，改配置只改这里。
注意：共用的是工厂函数；每个表任务是独立进程，各自 getOrCreate() 出自己的 session。
"""
import sys

from pyspark.sql import SparkSession

from config.settings import settings


def get_spark_session(app_name: str = "batch-pipeline") -> SparkSession:
    """创建配好 MinIO + StarRocks 的 SparkSession。"""
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
        # ---- StarRocks Spark Connector 1.1.4（Spark 4.x / Scala 2.13）+ MySQL JDBC 驱动 ----
        # connector 1.1.1+ 不再自带 MySQL 驱动，fe.jdbc.url 建连需要，必须一起加
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.3,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.782,"
                "com.starrocks:starrocks-spark-connector-4.1_2.13:1.1.4,"
                "com.mysql:mysql-connector-j:8.4.0")
        .getOrCreate()
    )
