"""表任务：ADS 每日大盘报表 → ClickHouse ads_daily_report。

职责：DWS 汇总 → 每日经营指标（基础上卷 + 派生比率 + 同环比 + 月累计）→ 写 ClickHouse。
调度：python -m src.pipelines.ads_daily_report 2026-09-04

注意：同环比/累计值需要多天数据，读 DWS 取「窗口最小依赖区间」[上月末, 当天]；
build 输出含区间内所有天的行，写入前只保留当天行（writer 只清当天分区）。
"""
from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import sys
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import Window
from src.io.clickhouse_reader import read_from_clickhouse
from src.io.clickhouse_writer import overwrite_partition_to_clickhouse
from src.spark import get_spark_session
from pyspark.sql.functions import (
    col,
    count,
    date_format,
    lag,
    max,
    round,
    sum,
)

SOURCE_TABLE = "dws_merchant_daily"
TARGET_TABLE = "ads_daily_report"


# ============ 纯业务逻辑（不碰 IO，可单测）============

def build(dws: DataFrame) -> DataFrame:
    """商户日汇总 → 每日大盘：上卷聚合 + 派生比率 + 同环比 + 月累计。

    输入：多天 DWS（dt <= 当天）；输出：每天一行（含历史）。
    """
    # ── 窗口定义 ──
    # 环比：大盘一天一行，全表按 dt 排序即是时间序列（partitionBy() = 单一全局分区）
    w_seq = Window.partitionBy().orderBy("dt")
    # 月累计：按自然月分组，从月初第一行累到当前行
    w_month = (
        Window.partitionBy("month").orderBy("dt")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (dws
        .withColumn("month", date_format(col("dt"), "yyyy-MM"))     # 自然月（月累计分组键）
        .groupBy("dt", "month")
        .agg(
            count("*").alias("active_merchant_count"),     # dws 一行=一个商户 → 行数=当天活跃商户数
            sum("txn_count").alias("total_txn_count"),
            sum("total_amount").alias("total_amount"),
            sum("pay_count").alias("pay_count"),
            sum("pay_amount").alias("pay_amount"),         # 全为 NULL 时结果为 NULL（sum 忽略 NULL）
            sum("refund_count").alias("refund_count"),
            sum("refund_amount").alias("refund_amount"),
            sum("cny_amount").alias("cny_amount"),
            sum("usd_amount").alias("usd_amount"),
            sum("eur_amount").alias("eur_amount"),
            sum("high_risk_txn_count").alias("high_risk_txn_count"),
            max("max_amount").alias("max_amount"),         # 极值上卷用 max，不是 sum
        )
        # ── ADS 特色：派生成品比率（下游直接展示，不用再算）──
        .withColumn("avg_ticket_amount",
                    round(col("total_amount") / col("total_txn_count"), 2))    # 客单价
        .withColumn("refund_rate",
                    round(col("refund_count") / col("total_txn_count"), 4))
        .withColumn("high_risk_txn_ratio",
                    round(col("high_risk_txn_count") / col("total_txn_count"), 4))
        # ── 同环比：与昨日比（lag 取上一行）──
        .withColumn("prev_refund_rate",
                    lag("refund_rate").over(w_seq))            # 历史第一天的昨日不存在 → NULL
        .withColumn("refund_rate_diff",
                    round(col("refund_rate") - col("prev_refund_rate"), 4))    # 环比变化量，正=恶化
        # ── 累计值：月初至今（rolling sum 窗口）──
        .withColumn("month_total_txn_count",
                    sum("total_txn_count").over(w_month))
        .withColumn("month_total_amount",
                    sum("total_amount").over(w_month))
    )


# ============ 任务入口（Airflow 调这个）============

def run(dt: str) -> None:
    """读 DWS（当天及以前）→ 大盘报表 → 只写当天分区。"""
    spark = get_spark_session(app_name=f"ads_daily_report_{dt}")

    # 窗口最小依赖区间：[上月末 1 天, 当天]
    #   月累计要月初→今天；同环比的 lag 在月初那天要取到上月末（否则 diff 静默 NULL）
    #   比「全历史」少读 90%+，比「只读当天」多 1 天的代价
    d = date.fromisoformat(dt)
    window_start = d.replace(day=1) - timedelta(days=1)     # 上月末一天
    days_span = (d - window_start).days + 1                 # 区间天数（含两端）
    dws = read_from_clickhouse(spark, SOURCE_TABLE, dt, days=days_span)
    ads_all = build(dws)

    # build 输出含区间内所有天的行；writer 只清当天分区，必须裁剪到当天再写，
    # 否则历史行被重复 append（主键模型 upsert 幂等但浪费写放大）
    ads_today = ads_all.filter(f"dt = '{dt}'")

    overwrite_partition_to_clickhouse(spark, ads_today, TARGET_TABLE, dt)

    spark.stop()
    logger.info("ADS 每日大盘完成", dt=dt, table=TARGET_TABLE)


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-09-04")
