"""表任务：DWD 交易明细宽表 → StarRocks dwd_transaction_offline。

职责：原始交易 → 清洗 → JOIN dim_customer / dim_merchant 打宽 → 校验 → 写 StarRocks。
调度：python -m src.pipelines.dwd_transaction 2026-09-04

模块内 clean / enrich_* 是纯函数（DF→DF），在 tests/ 里造假 DF 即可单测。
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.io.starrocks_writer import overwrite_partition_to_starrocks
from src.io.starrocks_reader import read_from_starrocks

from src.quality import check_dwd
from src.spark import get_spark_session

SOURCE_TABLE = "ods_transaction"
TARGET_TABLE = "dwd_transaction_offline"
APP_NAME = "batch_dwd_transaction"



# ============ 纯业务逻辑（不碰 IO，可单测）============

def clean(df: DataFrame) -> DataFrame:
    """清洗：过滤空值 / 非法金额 / 非法时间。"""
    df=(df.filter(col("transaction_id").isNotNull())
        .filter(col("amount") > 0)
        .filter(col("event_time").isNotNull())
        )
    return df


def enrich_customer(df: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """LEFT JOIN dim_customer，加 customer_level / customer_region。"""
    # TODO 步骤 5：
    # dim = dim_customer.selectExpr(
    #     "customer_id", "level as customer_level", "region as customer_region")
    # return df.join(dim, on="customer_id", how="left")
    dim =dim_customer.selectExpr(
        "customer_id", "level as customer_level", "region as customer_region",
        "register_time as customer_register_time"
    )
    return df.join(dim,on ="customer_id",how="left")


def enrich_merchant(df: DataFrame, dim_merchant: DataFrame) -> DataFrame:
    """LEFT JOIN dim_merchant，加 merchant_category / merchant_region / risk_level。"""
    # TODO 步骤 5：
    # dim = dim_merchant.selectExpr(
    #     "merchant_id", "category as merchant_category",
    #     "region as merchant_region", "risk_level")
    # return df.join(dim, on="merchant_id", how="left")
    dim=dim_merchant.selectExpr(
        "merchant_id", "category as merchant_category",
        "region as merchant_region", "risk_level as merchant_risk_level"
    )
    return df.join(dim,on ="merchant_id",how="left")


# ============ 任务入口（Airflow 调这个）============

def run(dt: str) -> None:
    """读 fact + dim → 清洗 → 打宽 → 校验 → 写 dwd_transaction_offline。"""
    spark = get_spark_session(app_name=f"{APP_NAME}_{dt}")

    raw = read_from_starrocks(spark, SOURCE_TABLE, dt)
    dim_customer = read_from_starrocks(spark, "dim_customer_offline")
    dim_merchant = read_from_starrocks(spark, "dim_merchant_offline")

    cleaned = clean(raw)
    enriched = enrich_customer(cleaned, dim_customer)
    enriched = enrich_merchant(enriched, dim_merchant)

    check_dwd(raw, enriched)
    overwrite_partition_to_starrocks(spark, enriched, TARGET_TABLE, dt)

    spark.stop()
    print(f"DWD {dt} 完成 → {TARGET_TABLE}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-09-04")
