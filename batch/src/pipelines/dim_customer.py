

from src.spark import get_spark_session
from src.io.starrocks_reader import read_from_starrocks
from src.io.starrocks_writer import overwrite_table_to_starrocks
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

SOURCE_TABLE = "ods_customer"
TARGET_TABLE = "dim_customer_offline"
SESSION_NAME= "batch_dim_customer"
def clean(df: DataFrame) -> DataFrame:
    """转换 DataFrame。"""
    #
    # 注意：StarRocks connector 会把 filter 下推成 SQL，length() 会被翻成 CHAR_LENGTH
    # 触发 MySQLSQLBuilder 不支持；只用 isNotNull / != "" 这类可下推的简单谓词
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
    df_raw = read_from_starrocks(spark, SOURCE_TABLE)
    #第二步 清理
    df = clean(df_raw)
    #第三步 校验
    df = validate(df)
    #第四步 写starrocks
    overwrite_table_to_starrocks(spark, df, TARGET_TABLE)
    print(f"DIM 完成：{df.count()} 条 → {TARGET_TABLE}")
    spark.stop()
if __name__ == "__main__":
    run()