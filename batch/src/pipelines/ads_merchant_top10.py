"""表任务：ADS 商户销售额 Top10 榜单 → StarRocks ads_merchant_top10_daily。

职责：DWS 商户日汇总 → 按 dt 窗口排名取 Top10 + 当日销售额占比 → 写 StarRocks。
调度：python -m src.pipelines.ads_merchant_top10 2026-09-04
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, round, sum

from src.io.starrocks_reader import read_from_starrocks
from src.io.starrocks_writer import overwrite_partition_to_starrocks
from src.spark import get_spark_session

SOURCE_TABLE = "dws_merchant_daily"
TARGET_TABLE = "ads_merchant_top10_daily"


# ============ 纯业务逻辑（不碰 IO，可单测）============

def build(dws: DataFrame) -> DataFrame:
    """商户日汇总 → 每日销售额 Top10 榜单（粒度：dt + rank）。

    注意与大盘的区别：输出粒度是 (dt, rank)，一天 10 行，所以独立建表，
    不能塞进一天一行的 ads_daily_report。
    """
    # 当天内按销售额降序，销售额并列时按笔数排（保证名次稳定可复现）
    w_rank = Window.partitionBy("dt").orderBy(col("total_amount").desc(), col("txn_count").desc())
    w_day = Window.partitionBy("dt")                                       # 当天全部商户（算占比分母）

    return (dws
        .withColumn("day_total_amount", sum("total_amount").over(w_day))   # 当日全平台销售额
        .withColumn("rank_no", row_number().over(w_rank))                  # 1,2,3...（并列不重复）
        .filter(col("rank_no") <= 10)                                      # 先编号后裁剪
        .withColumn("amount_share",
                    round(col("total_amount") / col("day_total_amount"), 4))   # 头部集中度：Top1 占比等
        .select("dt", "rank_no", "merchant_id", "txn_count", "customer_count",
                "total_amount", "amount_share")
    )


# ============ 任务入口（Airflow 调这个）============

def run(dt: str) -> None:
    """读 DWS 当天分区 → Top10 榜单 → 写当天分区。"""
    spark = get_spark_session(app_name=f"ads_merchant_top10_{dt}")

    dws = read_from_starrocks(spark, SOURCE_TABLE, dt)   # 榜单只看当天，不需要历史
    top10 = build(dws)

    overwrite_partition_to_starrocks(spark, top10, TARGET_TABLE, dt)

    spark.stop()
    print(f"ADS 商户Top10 {dt} 完成 → {TARGET_TABLE}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-09-04")
