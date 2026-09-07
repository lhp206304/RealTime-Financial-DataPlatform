"""表任务：ODS 商户维度原样落地 → StarRocks dim_merchant。

职责：MinIO dim 桶 dim_merchant.parquet → 类型对齐 + 校验 → 写 StarRocks（整表覆盖）。
调度：python -m src.pipelines.ods_merchant
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.io.minio_reader import read_minio
from src.io.starrocks_writer import overwrite_table_to_starrocks
from src.quality import check_ods
from src.spark import get_spark_session

SOURCE_TABLE = "dim_merchant"
TARGET_TABLE = "ods_merchant"

VALID_RISK_LEVELS = ["LOW", "MEDIUM", "HIGH"]   # RiskLevel 枚举值


def transform(df: DataFrame) -> DataFrame:
    """ODS 不清洗不过滤；商户维度无时间字段，只原样落地。"""
    return df


def check(df: DataFrame) -> None:
    """risk_level 必须满足 RiskLevel 枚举值。"""
    bad_level = df.filter(~col("risk_level").isin(VALID_RISK_LEVELS)).count()
    assert bad_level == 0, f"{bad_level} 行 risk_level 不在 {VALID_RISK_LEVELS} 内"


def run(dt: str | None = None) -> None:
    spark = get_spark_session("batch_ods_merchant")
    raw = read_minio(spark, "dim", SOURCE_TABLE)
    raw_count = raw.count()

    ods = transform(raw)
    check(ods)
    check_ods(raw_count, ods.count())

    overwrite_table_to_starrocks(spark, ods, TARGET_TABLE)   # 维表无分区：整表覆盖
    spark.stop()
    print(f"ODS 商户维度完成：{raw_count} 条 → {TARGET_TABLE}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else None)
