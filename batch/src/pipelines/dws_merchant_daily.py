"""表任务：DWS 商户日汇总 → ClickHouse dws_merchant_daily。

职责：DWD 宽表 → 按 商户+天 聚合（笔数/金额/均额）→ 校验 → 写 ClickHouse。
调度：python -m src.pipelines.dws_merchant_daily 2026-09-04
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count, countDistinct, max, sum,when,col,round

from src.io.clickhouse_reader import read_from_clickhouse
from src.io.clickhouse_writer import overwrite_partition_to_clickhouse
from src.quality import check_dws
from src.spark import get_spark_session

SOURCE_TABLE = "dwd_transaction_offline"
TARGET_TABLE = "dws_merchant_daily"
APP_NAME='dws_merchant_daily'

# ============ 纯业务逻辑（不碰 IO，可单测）============

def aggregate(dwd: DataFrame) -> DataFrame:
    """groupBy(merchant_id, dt) → 规模/交易类型/币种/风险指标 + 退款率。"""
    return (dwd
        .groupBy("merchant_id", "dt")       # dt 列由 ODS/DWD 继承（DATE 类型）
        .agg(
            # ── 规模指标 ──
            count("*").alias("txn_count"),                          # 交易笔数
            countDistinct("customer_id").alias("customer_count"),   # 去重客户数
            countDistinct("account_id").alias("account_count"),     # 去重账户数
            sum("amount").alias("total_amount"),                    # 交易总额
            avg("amount").alias("avg_amount"),                      # 平均单笔金额（Spark avg → DOUBLE）
            max("amount").alias("max_amount"),                      # 单笔最大金额
            # ── 交易类型（码值 PAYMENT/REFUND）──
            count(when(col("transaction_type") == "PAYMENT", 1)).alias("pay_count"),          # count 无匹配得 0
            sum(when(col("transaction_type") == "PAYMENT", col("amount"))).alias("pay_amount"),   # sum 无匹配得 NULL
            count(when(col("transaction_type") == "REFUND", 1)).alias("refund_count"),
            sum(when(col("transaction_type") == "REFUND", col("amount"))).alias("refund_amount"),
            # ── 币种（码值 CNY/USD/EUR，混币种 sum 无意义 → 按币种拆列）──
            countDistinct("currency").alias("currency_count"),
            sum(when(col("currency") == "CNY", col("amount"))).alias("cny_amount"),
            sum(when(col("currency") == "USD", col("amount"))).alias("usd_amount"),
            sum(when(col("currency") == "EUR", col("amount"))).alias("eur_amount"),
            # ── 风险（码值 LOW/MEDIUM/HIGH，JOIN dim_merchant 带入的商户风险等级）──
            count(when(col("merchant_risk_level") == "HIGH", 1)).alias("high_risk_txn_count"),
            countDistinct(when(col("merchant_risk_level") == "HIGH", col("customer_id")))
                .alias("high_risk_customer_count"),                 # 在高风险商户交易过的去重客户数
        )
        # ── 派生比率（聚合结果列之间计算，不进 .agg）──
        .withColumn("refund_rate",
                    round(col("refund_count") / col("txn_count"), 4)))  # 退款笔数占比；txn_count≥1 不会除零


# ============ 任务入口（Airflow 调这个）============

def run(dt: str) -> None:
    """读 DWD → 聚合 → 校验 → 写 dws_merchant_daily。"""
    spark = get_spark_session(app_name=f"{APP_NAME}_{dt}")

    dwd = read_from_clickhouse(spark, SOURCE_TABLE, dt)
    dws = aggregate(dwd)

    check_dws(dwd, dws)
    overwrite_partition_to_clickhouse(spark, dws, TARGET_TABLE, dt)

    spark.stop()
    print(f"DWS 商户日汇总 {dt} 完成 → {TARGET_TABLE}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-09-04")
