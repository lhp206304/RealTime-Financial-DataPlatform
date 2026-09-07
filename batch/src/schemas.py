"""DataFrame Schema（StructType）—— 和 generator/schema.py 的 Pydantic 模型对齐。

PySpark 读 Parquet 时 schema 一般自动推断，这里定义是给测试和校验用。
"""
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# 事实表：交易
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("currency", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("event_time", TimestampType(), False),
])

# 维度表：客户
DIM_CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("level", StringType(), False),
    StructField("region", StringType(), False),
    StructField("register_time", TimestampType(), False),
])

# 维度表：商户
DIM_MERCHANT_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("region", StringType(), False),
    StructField("risk_level", StringType(), False),
])
