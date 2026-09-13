from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import yaml
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 作业侧的统一配置（batch/config/settings.py，通过 /opt/jobs 挂载进 airflow 容器）
from config.settings import settings

# 1 读取yaml文件
yaml_file = Path(__file__).parent/"config/warehouse_tables.yml"
with open(yaml_file, "r") as f:
    config = yaml.safe_load(f)


# 2 解析yaml文件
all_tasks={}

# 连接信息全部从 settings 读（环境变量注入，与 api/compose 同一套变量名）；
# 只有「部署绑定」类参数硬编码在这：driver 地址、executor 的 Python 路径。
spark_conf = {
    "spark.pyspark.python": "/usr/bin/python3",
    "spark.pyspark.driver.python": "/usr/bin/python3",
    # client 模式下 driver 跑在 scheduler 容器里，executor 在 spark-worker 上，
    # 必须显式告诉 executor 回连 driver 的地址（用 compose 服务名，容器间可解析）
    "spark.driver.host": "airflow-scheduler",
    # MinIO S3A
    "spark.hadoop.fs.s3a.endpoint": settings.minio_endpoint,
    "spark.hadoop.fs.s3a.access.key": settings.minio_access_key,
    "spark.hadoop.fs.s3a.secret.key": settings.minio_secret_key,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    # ClickHouse catalog
    "spark.sql.catalog.clickhouse": "com.clickhouse.spark.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host": settings.clickhouse_host,
    "spark.sql.catalog.clickhouse.protocol": "http",
    "spark.sql.catalog.clickhouse.http_port": str(settings.clickhouse_http_port),
    "spark.sql.catalog.clickhouse.user": settings.clickhouse_user,
    "spark.sql.catalog.clickhouse.password": settings.clickhouse_password,
    "spark.sql.catalog.clickhouse.database": settings.clickhouse_database,
}
common = dict(
    # master 来自 spark_default 连接（docker-compose 里用 AIRFLOW_CONN_SPARK_DEFAULT 注入）
    conn_id="spark_default",
    application_args=["{{ ds }}"],  # 业务日期传给脚本
    conf=spark_conf,
)
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": 300,  # 5 分钟
}
with DAG(
    dag_id="warehouse_dynamic",
    description="离线批链路：ODS → DWD → DWS → ADS → Redis",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["batch", "spark"],
) as dag:
    for section, task_tables in config.items():
        for task_table in task_tables:
            if task_table.get("command"):
                # 通用 Bash 任务（如 generator 造数）：命令和工作目录都在 yml 里声明
                task = BashOperator(
                    task_id=task_table["task_id"],
                    bash_command=task_table["command"],
                    cwd=task_table.get("cwd", "/opt/jobs"),
                )
            elif task_table.get("bash_command"):
                # batch 内置纯 Python 任务（sync_dim_redis）：默认在 /opt/jobs 下跑
                task = BashOperator(
                    task_id=task_table["task_id"],
                    bash_command=task_table["bash_command"],
                    cwd=task_table.get("cwd", "/opt/jobs"),
                )
            else:
                task = SparkSubmitOperator(
                    task_id=task_table["task_id"],
                    application=task_table["application"],
                    **common,
                )
            all_tasks[task_table["task_id"]] = task
# 3 动态生成任务依赖 
    for task_tables in config.values():
        for task_table in task_tables:
            task=all_tasks[task_table["task_id"]]
            upstream_ids=task_table.get("dependencies", [])
            for upstream_id in upstream_ids:
                upstream_task=all_tasks[upstream_id]
                upstream_task >> task
    
    # 如果当前层级没有依赖，直接把上个层级所有任务作为上游
    sections = list(config.keys())
    for i in range(1,len(sections)):
        pre_section=sections[i-1]
        curr_section=sections[i]
        all_pre_section_tasks = [all_tasks[t["task_id"]]for t in config[pre_section]]
        
        for task_table in config[curr_section]:
            if not task_table.get("dependencies"):
                task=all_tasks[task_table["task_id"]]
                all_pre_section_tasks >> task

if __name__ == "__main__":
    logger.info("config type", config_type=type(config))
    logger.info("config content", config=config)
    for task_tables in config.values():
        for task_table in task_tables:
            logger.info("task table", task_table=task_table)