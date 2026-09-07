"""表任务：DWS 客户日汇总 → ClickHouse dws_customer_daily。

职责：DWD 宽表 → 按 客户+天 聚合（笔数/金额/均额）→ 校验 → 写 ClickHouse。
调度：python -m src.pipelines.dws_customer_daily 2026-09-04
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum, avg, when

from src.io.clickhouse_reader import read_from_clickhouse
from src.io.clickhouse_writer import overwrite_partition_to_clickhouse
from src.quality import check_dws
from src.spark import get_spark_session

SOURCE_TABLE = "dwd_transaction_offline"
TARGET_TABLE = "dws_customer_daily"
APP_NAME='dws_customer_daily'

# ============ 纯业务逻辑（不碰 IO，可单测）============

def aggregate(dwd: DataFrame) -> DataFrame:
    """groupBy(customer_id, dt) → 交易笔数 / 总金额 / 平均金额 + 支付/退款条件聚合。"""
    return (
        dwd
        .groupBy("customer_id", "dt")          # dt 列由 ODS/DWD 继承（DATE 类型）
        .agg(
            count("*").alias("txn_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            # 条件聚合：transaction_type 信息保留成列
            count(when(col("transaction_type") == "PAYMENT", 1)).alias("pay_count"),
            sum(when(col("transaction_type") == "PAYMENT", col("amount")).otherwise(0)).alias("pay_amount"),
            count(when(col("transaction_type") == "REFUND", 1)).alias("refund_count"),
            sum(when(col("transaction_type") == "REFUND", col("amount")).otherwise(0)).alias("refund_amount"),
        )
    )

# ============ 任务入口（Airflow 调这个）============

def run(dt: str) -> None:
    """读 DWD → 聚合 → 校验 → 写 dws_customer_daily。"""
    spark = get_spark_session(app_name=f"{APP_NAME}_{dt}")

    dwd = read_from_clickhouse(spark, SOURCE_TABLE, dt)
    dws = aggregate(dwd)

    check_dws(dwd, dws)
    overwrite_partition_to_clickhouse(spark, dws, TARGET_TABLE, dt)

    spark.stop()
    print(f"DWS 客户日汇总 {dt} 完成 → {TARGET_TABLE}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-09-04")
