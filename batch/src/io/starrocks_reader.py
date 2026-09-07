"""读 StarRocks：下层任务回读上层结果（如 DWS 读 DWD 宽表）。

用 StarRocks Spark Connector（和 writer 同一个 jar）。
严格分层时 DWS/ADS 从这里读，保证层与层解耦。
"""
from pyspark.sql import DataFrame, SparkSession

from config.settings import settings


def read_from_starrocks(spark: SparkSession, table: str,dt:str|None=None , days:int=1) -> DataFrame:
    """读 StarRocks 表回 DataFrame。

    table: 表名，如 'dwd_transaction_offline'
    dt: 业务日期 'YYYY-MM-DD'，None 读全表（维表）
    """
    reader = (
        spark.read.format("starrocks")
        .option("starrocks.fe.http.url", f"{settings.starrocks_host}:{settings.starrocks_port}")
        .option("starrocks.fe.jdbc.url",
                f"jdbc:mysql://{settings.starrocks_host}:{settings.starrocks_query_port}")
        .option("starrocks.table.identifier", f"{settings.starrocks_database}.{table}")
        .option("starrocks.user", settings.starrocks_user)
        .option("starrocks.password", settings.starrocks_password)
    )
    # dt 为空读全表；不为空按业务日期下推过滤（事实表，只读当天分区）。
    # 用 filter.query 传 WHERE 条件，不用 starrocks.partition：
    # 后者要填 StarRocks 分区名，date_trunc 表达式分区的名字是自动生成的 p20260904，不是 dt=...
    if dt:
        if days <= 1:
            reader = reader.option("starrocks.filter.query", f"dt = '{dt}'")
        else:
            start = (datetime.strptime(dt, "%Y-%m-%d") - timedelta(days=days - 1)).strftime("%Y-%m-%d")
            reader = reader.option("starrocks.filter.query", f"dt >= '{start}' AND dt <= '{dt}'")
    return reader.load()
