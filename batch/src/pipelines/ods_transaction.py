"""表任务：ODS 交易明细原样落地 → ClickHouse ods_transaction。

职责：MinIO fact 桶原始交易 → 不加工（最多类型对齐）→ 写 ClickHouse。
调度：python -m src.pipelines.ods_transaction 2026-09-04（不传日期 = 全量）
"""
import sys

from pyspark.sql import DataFrame

from src.io.minio_reader import read_minio
from src.io.clickhouse_writer import (
    overwrite_partition_to_clickhouse,
    overwrite_table_to_clickhouse,
)
from src.quality import check_ods
from src.spark import get_spark_session

TARGET_TABLE = "ods_transaction"
SOURCE_TABLE = "fact_transaction"



def transform(df: DataFrame) -> DataFrame:
    """ODS 不清洗不过滤，忠实落地；做字段类型对齐 + 派生 dt 分区列。"""
    from pyspark.sql.functions import col, to_date
    return (df
        .withColumn("event_time", col("event_time").cast("timestamp"))
        .withColumn("amount", col("amount").cast("decimal(10,2)"))
        .withColumn("dt", to_date(col("event_time")))   # 分区列：所有事实表统一按 dt 分区
    )


def run(dt: str | None = None) -> None:
    """任务入口：读 fact 桶 → 原样 → 校验 → 写 ods_transaction。"""
    spark=get_spark_session("batch_ods_transaction")
    raw = read_minio(spark, "fact", SOURCE_TABLE, dt)
    raw_count = raw.count()

    ods = transform(raw)
    check_ods(raw_count, ods.count())
    if dt:
        overwrite_partition_to_clickhouse(spark, ods, TARGET_TABLE, dt)  # 日批：覆盖当天分区
    else:
        overwrite_table_to_clickhouse(spark, ods, TARGET_TABLE)         # 不传 dt：首次全量初始化

    spark.stop()
    print(f"ODS {dt or '全量'} 完成：{raw_count} 条 → {TARGET_TABLE}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else None)
