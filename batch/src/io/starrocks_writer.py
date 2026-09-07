"""写 StarRocks：Spark DataFrame → StarRocks 表（StarRocks Spark Connector / Stream Load）。

两个写入入口，按目标表类型选：
    overwrite_partition_to_starrocks  分区事实表（ODS/DWD/DWS/ADS）：删当天数据再 append，支持按天重跑
    overwrite_table_to_starrocks      无分区维表（dim_*）：整表 truncate 后全量重写

connector 本身不会自动建表：写入前由 _write 调 ensure_table，按
starrocks/ddl/{表名}.sql 自动建库建表（CREATE TABLE IF NOT EXISTS，幂等）。
"""
from pyspark.sql import DataFrame, SparkSession

from config.settings import settings
from src.io.starrocks_admin import ensure_table, execute_sql


def _write(df: DataFrame, table: str, write_mode: str) -> None:
    """Stream Load 写入本体（df 自带 SparkSession，不需要额外传 spark）。

    写入前先 ensure_table：库/表不存在则按 starrocks/ddl/{table}.sql 自动建好（幂等）。
    """
    ensure_table(table)
    (df.write.format("starrocks")
        .option("starrocks.fe.http.url", f"{settings.starrocks_host}:{settings.starrocks_port}")
        .option("starrocks.fe.jdbc.url",
                f"jdbc:mysql://{settings.starrocks_host}:{settings.starrocks_query_port}")
        .option("starrocks.table.identifier", f"{settings.starrocks_database}.{table}")
        .option("starrocks.user", settings.starrocks_user)
        .option("starrocks.password", settings.starrocks_password)
        .mode(write_mode)
        .save())


def overwrite_partition_to_starrocks(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    dt: str,
) -> None:
    """按天分区表写入：先删当天分区再 append（同一天重跑幂等，不产生重复）。

    用于事实表：ods_transaction / dwd_transaction_offline / dws_* / ads_*。
    约定：所有分区表统一按 dt 列（'YYYY-MM-DD'）分区，df 里必须带 dt 列。
    table: 目标表名
    dt:    业务日期 'YYYY-MM-DD'
    """
    # 先确保表存在（首次运行表还没建，DELETE/TRUNCATE 会报错），再删数据
    ensure_table(table)
    # DELETE 走 FE MySQL 端口 9030（pymysql），不走 Spark
    execute_sql(
        f"DELETE FROM {settings.starrocks_database}.{table} WHERE dt = '{dt}'",
        database=settings.starrocks_database,
    )
    _write(df, table, write_mode="append")
    print(f"写入 StarRocks {table} 分区 dt={dt} 完成")


def overwrite_table_to_starrocks(
    spark: SparkSession,
    df: DataFrame,
    table: str,
) -> None:
    """整表全量重写：TRUNCATE 清空后 append（无分区维表 T+1 整刷）。

    用于维表：dim_customer / dim_merchant。
    注意：StarRocks connector 的 mode("overwrite") 不支持 truncate，
    所以显式 TRUNCATE 后再 append（都走 FE 9030 / Stream Load）。
    StarRocks 无 TRUNCATE TABLE IF EXISTS 语法，先 ensure_table 建表再 TRUNCATE。
    """
    ensure_table(table)
    execute_sql(
        f"TRUNCATE TABLE {settings.starrocks_database}.{table}",
        database=settings.starrocks_database,
    )
    _write(df, table, write_mode="append")
    print(f"全量重写 StarRocks {table} 完成")
