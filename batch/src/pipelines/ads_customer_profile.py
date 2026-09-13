"""表任务：ADS 客户当天画像 → ClickHouse ads_customer_profile。

职责：DWS 客户日汇总（当天分区）→ 派生成品指标 + 行为打标 + 价值分层 → 写 ClickHouse。
调度：python -m src.pipelines.ads_customer_profile 2026-09-04

粒度说明：DWS 当天分区一行 = 一个客户（customer_id, dt 粒度），
因此本层不需要再 groupBy 聚合，只做「透传 + 派生 + 打标」。
"""
from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, coalesce, lag, lit, round, when

from src.io.clickhouse_reader import read_from_clickhouse
from src.io.clickhouse_writer import overwrite_partition_to_clickhouse
from src.spark import get_spark_session
from pyspark.sql.window import Window


SOURCE_TABLE = "dws_customer_daily"
TARGET_TABLE = "ads_customer_profile"


# ============ 纯业务逻辑（不碰 IO，可单测）============

def build(dws: DataFrame) -> DataFrame:
    """客户当天画像：派生成品指标（下游直接展示）+ 行为打标 + 价值分层。"""
    # 带 frame 的窗口：给 avg 用（滑动区间 = 前 6 行 + 当前行）
    w_7d = Window.partitionBy("customer_id").orderBy("dt").rowsBetween(-6, 0)
    # 不带 frame 的窗口：给 lag 用（lag/lead 不允许指定 frame，只按分区排序取偏移行）
    w_prev = Window.partitionBy("customer_id").orderBy("dt")

    return (dws
        # ── 透传当天核心指标（DWS 已算好的，直接带过来）──
        .select(
            "customer_id", "dt",
            "txn_count", "total_amount",      # 当天笔数 / 当天金额
            "pay_count", "pay_amount",
            "refund_count", "refund_amount",
        )
        # ── 派生成品比率（当天口径，聚合结果列之间重算）──
        .withColumn("avg_ticket_amount",
                    round(col("total_amount") / col("txn_count"), 2))               # 当天客单价
        .withColumn("refund_txn_rate",
                    round(col("refund_count") / col("txn_count"), 4))               # 当天退款笔数占比
        .withColumn("pay_amount_ratio",
                    round(coalesce(col("pay_amount"), lit(0)) / col("total_amount"), 4))  # 支付金额占比（无支付按 0）
        # ── 行为打标（when/otherwise → 布尔，看板筛选用）──
        .withColumn("has_refund", when(col("refund_count") > 0, True).otherwise(False))  # 当天是否发生过退款
        # ── 价值分层（多分支 when：按当天交易额分档，风控/营销常用）──
        .withColumn("amount_tier",
                    when(col("total_amount") >= 10000, "HIGH")       # 高价值
                    .when(col("total_amount") >= 1000, "MID")        # 中价值
                    .otherwise("LOW"))                               # 普通
        # 7 日移动平均
        .withColumn("ma7_total_amount", avg("total_amount").over(w_7d))
        .withColumn("ma7_txn_count", avg("txn_count").over(w_7d))
        # 日环比（lag 取昨天）
        .withColumn("dod_amount_change", col("total_amount") - lag("total_amount", 1).over(w_prev))
    )


# ============ 任务入口（Airflow 调这个）============

def run(dt: str) -> None:
    """读 DWS 当天分区 → 当天客户画像 → 写当天 dt 分区。"""
    spark = get_spark_session(app_name=f"ads_customer_profile_{dt}")

    dws = read_from_clickhouse(spark, SOURCE_TABLE, dt, days=7)   # 读最近 7 天数据（客户日汇总）
    ads = build(dws).filter(col("dt") == dt)

    overwrite_partition_to_clickhouse(spark, ads, TARGET_TABLE, dt)

    spark.stop()
    logger.info("ADS 客户画像完成", dt=dt, table=TARGET_TABLE)


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-09-04")
