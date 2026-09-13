
"""表任务：ODS 客户维度原样落地 → ClickHouse ods_customer。

职责：MinIO dim 桶 dim_customer.parquet → 类型对齐 + 校验 → 写 ClickHouse（整表覆盖）。
调度：python -m src.pipelines.ods_customer
"""
from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

from src.io.minio_reader import read_minio
from src.io.clickhouse_writer import overwrite_table_to_clickhouse
from src.quality import check_ods
from src.spark import get_spark_session

SOURCE_TABLE = "dim_customer"
TARGET_TABLE = "ods_customer"
VALID_LEVELS = ["V1", "V2", "V3", "V4"]   # CustomerLevel 枚举值
VALID_STATUS = ["ACTIVE", "DELETED"]       # DimStatus 枚举值（软删标记）


def transform(df: DataFrame) -> DataFrame:
    """ODS 不清洗不过滤；做 register_time 类型对齐（转不了 → NULL，交给校验拦截）。"""
    return df.withColumn("register_time", to_timestamp(col("register_time")))


def check(df: DataFrame) -> None:
    """register_time 必须能转成时间；level 必须在 CustomerLevel 枚举内。"""
    bad_time = df.filter(col("register_time").isNull()).count()
    bad_level = df.filter(~col("level").isin(VALID_LEVELS)).count()
    bad_status = df.filter(~col("status").isin(VALID_STATUS)).count()
    assert bad_time == 0, f"{bad_time} 行 register_time 转不了时间"
    assert bad_level == 0, f"{bad_level} 行 level 不在 {VALID_LEVELS} 内"
    assert bad_status == 0, f"{bad_status} 行 status 不在 {VALID_STATUS} 内"


def run(dt: str | None = None) -> None:
    spark = get_spark_session("batch_ods_customer")
    raw = read_minio(spark, "dim", SOURCE_TABLE)   # 维表全量读，不传 dt
    raw_count = raw.count()

    ods = transform(raw)
    check(ods)
    check_ods(raw_count, ods.count())

    overwrite_table_to_clickhouse(spark, ods, TARGET_TABLE)   # 维表无分区：整表覆盖
    spark.stop()
    logger.info("ODS 客户维度完成", rows=raw_count, table=TARGET_TABLE)


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else None)
