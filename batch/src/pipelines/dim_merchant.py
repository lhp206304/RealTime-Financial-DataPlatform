from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

from src.spark import get_spark_session
from src.io.clickhouse_reader import read_from_clickhouse
from src.io.clickhouse_writer import overwrite_table_to_clickhouse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

SOURCE_TABLE = "ods_merchant"
TARGET_TABLE = "dim_merchant_offline"
SESSION_NAME = "batch_dim_merchant"

def clean(df: DataFrame) -> DataFrame:
    """转换 DataFrame。"""
    #
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
    logger.info("DIM 商户完成", rows=df.count(), table=TARGET_TABLE)
    spark.stop()
if __name__ == "__main__":
    run()
