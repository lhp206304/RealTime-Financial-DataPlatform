import yaml
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 1 读取yaml文件
yaml_file = Path(__file__).parent/"config/warehouse_tables.yml"
with open(yaml_file, "r") as f:
    config = yaml.safe_load(f)


# 2 解析yaml文件
all_tasks={}

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
            if task_table.get("bash_command"):
                task= BashOperator(
                    task_id=task_table["task_id"], 
                    bash_command=f"python3 -m src.pipelines.{task_table['task_id']}",
                    cwd="/opt/jobs",
                )
            else:
                task= SparkSubmitOperator(
                    task_id=task_table["task_id"], 
                    application=task_table["application"],
                    **common,
                )
            all_tasks[task_table["task_id"]] =task
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
    print(type(config))
    print(config)
    print("***********\n")
    for task_tables in config.values():
        for task_table in task_tables:
            print(task_table)
            print("###########\n")