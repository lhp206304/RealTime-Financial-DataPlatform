from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

from src.spark import get_spark_session
from src.io.clickhouse_reader import read_from_clickhouse
from src.io.clickhouse_writer import overwrite_table_to_clickhouse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

SOURCE_TABLE = "ods_customer"
TARGET_TABLE = "dim_customer_offline"
SESSION_NAME= "batch_dim_customer"
def clean(df: DataFrame) -> DataFrame:
    """转换 DataFrame。"""
    #
    # 只用 isNotNull / != "" 这类简单谓词（旧 StarRocks connector 下 length() 无法下推，
    # 迁 ClickHouse 后无此限制，但过滤逻辑保持不变）
    df=(df.filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
        .filter(col("level").isNotNull() & (col("level") != ""))
        .filter(col("register_time").isNotNull())        # 时间列：只有 NULL，没有空串
    )
    return df
def validate(df: DataFrame) -> DataFrame:
    """校验 DataFrame。"""
    # 校验数据是否为空
    assert not df.isEmpty(), "数据为空"
    return df
def run(dt: str|None=None):
    #读取ods数据
    spark=get_spark_session(SESSION_NAME)
    df_raw = read_from_clickhouse(spark, SOURCE_TABLE)
    #第二步 清理
    df = clean(df_raw)
    #第三步 校验
    df = validate(df)
    #第四步 写clickhouse
    overwrite_table_to_clickhouse(spark, df, TARGET_TABLE)
    logger.info("DIM 客户完成", rows=df.count(), table=TARGET_TABLE)
    spark.stop()
if __name__ == "__main__":
    run()
