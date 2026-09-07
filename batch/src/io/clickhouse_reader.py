"""读 ClickHouse：下层任务回读上层结果（如 DWS 读 DWD 宽表）。

与旧方案的区别：
    旧：clickhouse-connect 全量查成 Pandas → createDataFrame（数据全过 driver）
    新：spark.table(...) 走 connector catalog，filter 谓词下推到 ClickHouse，
        ClickHouse 返回过滤后的分区数据，executor 并行读。

关于去重：表引擎 ReplacingMergeTree 的后台 merge 有延迟，理论上读到的
数据可能含未 merge 的重复行。本项目写入侧是「先删后插」的覆盖模式
（overwritePartitions / 整表覆盖），表内本来就不存在重复 key，
因此这里不需要 FINAL / 去重。（sync_dim_redis 用 HTTP 直读才带 FINAL 兜底）
"""
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import settings


def read_from_clickhouse(
    spark: SparkSession,
    table: str,
    dt: str | None = None,
    days: int = 1,
) -> DataFrame:
    """读 ClickHouse 表回 Spark DataFrame（分布式 + 谓词下推）。

    table: 表名，如 'dwd_transaction_offline'（自动挂到 clickhouse.finance 下）
    dt:    业务日期 'YYYY-MM-DD'，None 读全表（维表）
    days:  读最近 N 天（含 dt 当天），1 = 只读 dt 当天；dt 为 None 时忽略
    """
    df = spark.table(f"clickhouse.{settings.clickhouse_database}.{table}")

    # dt 为空读全表；不为空只取需要的天（filter 会下推成 ClickHouse WHERE，减少读取量）
    if dt:
        if days <= 1:
            df = df.filter(F.col("dt") == F.lit(dt))
        else:
            start = (date.fromisoformat(dt) - timedelta(days=days - 1)).isoformat()
            df = df.filter((F.col("dt") >= F.lit(start)) & (F.col("dt") <= F.lit(dt)))

    return df
