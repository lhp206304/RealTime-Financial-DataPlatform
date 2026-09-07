"""写 ClickHouse：Spark 分布式写入（ClickHouse Spark Connector）。

与旧方案的区别：
    旧：df.toPandas() 把全部数据拉到 driver 单机 HTTP 写（数据量大时 OOM）
    新：df.writeTo(...) 走 connector，每个 partition 由 executor 并发写 ClickHouse

两个写入入口：
    overwrite_partition_to_clickhouse  分区事实表：动态分区覆盖（幂等重跑）
    overwrite_table_to_clickhouse      无分区维表：整表覆盖（TRUNCATE 语义）

表引用统一用三段名：clickhouse.finance.{table}
    clickhouse = src/spark.py 里注册的 catalog 名
    finance    = settings.clickhouse_database
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from src.io.clickhouse_admin import ensure_table

CATALOG = "clickhouse"


def _qualified(table: str) -> str:
    """表名 → catalog.database.table 三段全名。"""
    from config.settings import settings
    return f"{CATALOG}.{settings.clickhouse_database}.{table}"


def overwrite_partition_to_clickhouse(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    dt: str,
) -> None:
    """按天分区表写入：按 dt 表达式覆盖（幂等重跑）。

    用于：ods_transaction / dwd_transaction_offline / dws_* / ads_*
    注意：不能用 overwritePartitions()（动态分区覆盖）—— SCC 未实现该接口，
    会报 UNSUPPORTED_FEATURE.TABLE_OPERATION "does not support dynamic overwrite"。
    overwrite(dt 条件) 语义：connector 先删 WHERE dt=... 的行，再插入 df，
    等价旧方案 DELETE+append，但删和写在集群侧一次完成。
    """
    ensure_table(table)
    df.writeTo(_qualified(table)).overwrite(col("dt") == lit(dt))
    print(f"写入 ClickHouse {table} 分区 dt={dt} 完成（Spark 分布式）")


def overwrite_table_to_clickhouse(
    spark: SparkSession,
    df: DataFrame,
    table: str,
) -> None:
    """整表全量重写（无分区维表 T+1 整刷）。

    用于：dim_customer_offline / dim_merchant_offline
    overwrite(lit(True)) = OverwriteByExpression(WHERE true)：
    connector 先删全表再插入，等价旧方案的 TRUNCATE+append。
    """
    ensure_table(table)
    df.writeTo(_qualified(table)).overwrite(lit(True))
    print(f"全量重写 ClickHouse {table} 完成（Spark 分布式）")
