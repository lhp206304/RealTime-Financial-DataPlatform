from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": 300,  # 5 分钟
}

with DAG(
    dag_id="batch_daily",
    description="离线批链路：ODS → DWD → DWS → ADS → Redis",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["batch", "spark"],
) as dag:

    # 公共配置
    spark_conf = {
        "spark.pyspark.python": "/usr/bin/python3",
        "spark.pyspark.driver.python": "/usr/bin/python3",
        # MinIO S3A
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # ClickHouse catalog
        "spark.sql.catalog.clickhouse": "com.clickhouse.spark.ClickHouseCatalog",
        "spark.sql.catalog.clickhouse.host": "clickhouse",
        "spark.sql.catalog.clickhouse.protocol": "http",
        "spark.sql.catalog.clickhouse.http_port": "8123",
        "spark.sql.catalog.clickhouse.user": "default",
        "spark.sql.catalog.clickhouse.password": "",
        "spark.sql.catalog.clickhouse.database": "finance",
    }

    common = dict(
        conn_id="spark_default",       # Connection master=spark://spark-master:7077，须与 settings.spark_master 一致
        application_args=["{{ ds }}"],  # 业务日期传给脚本
        conf=spark_conf,
    )

    ods_transaction = SparkSubmitOperator(
        task_id="ods_transaction",
        application="/opt/jobs/src/pipelines/ods_transaction.py",
        **common,
    )

    dwd_transaction = SparkSubmitOperator(
        task_id="dwd_transaction",
        application="/opt/jobs/src/pipelines/dwd_transaction.py",
        **common,
    )

    dws_customer_daily = SparkSubmitOperator(
        task_id="dws_customer_daily",
        application="/opt/jobs/src/pipelines/dws_customer_daily.py",
        **common,
    )

    dws_merchant_daily = SparkSubmitOperator(
        task_id="dws_merchant_daily",
        application="/opt/jobs/src/pipelines/dws_merchant_daily.py",
        **common,
    )

    ads_customer_profile = SparkSubmitOperator(
        task_id="ads_customer_profile",
        application="/opt/jobs/src/pipelines/ads_customer_profile.py",
        **common,
    )

    ads_daily_report = SparkSubmitOperator(
        task_id="ads_daily_report",
        application="/opt/jobs/src/pipelines/ads_daily_report.py",
        **common,
    )

    # sync_dim_redis 是纯 Python，不走 Spark，用 BashOperator
    from airflow.operators.bash import BashOperator
    sync_dim_redis = BashOperator(
        task_id="sync_dim_redis",
        bash_command="python3 -m src.pipelines.sync_dim_redis",
        cwd="/opt/jobs",
    )

    # 依赖关系：ods → dwd → dws(并行) → ads(并行) → sync_redis
    ods_transaction >> dwd_transaction >> [dws_customer_daily, dws_merchant_daily]
    dws_customer_daily >> ads_customer_profile 
    dws_merchant_daily >> ads_daily_report
    [ads_customer_profile,ads_daily_report] >> sync_dim_redis
