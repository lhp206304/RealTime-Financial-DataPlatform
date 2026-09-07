"""表任务里纯业务函数的单测：造假 DataFrame 进来，不连 MinIO/ClickHouse。

测的是 pipelines 模块里的 clean / aggregate / build 等纯函数（DF→DF），
不测 run()（run 要连外部存储，属于集成测试）。
"""

from datetime import datetime


def test_clean_filters_negative_amount(spark):
    """DWD clean：负金额 / 零金额应被过滤。"""
    df = spark.createDataFrame(
        [
            ("T1", -1, datetime(2026, 9, 4, 10), "C1"),
            ("T2", 100, datetime(2026, 9, 4, 11), "C2"),
            ("T3", 0, datetime(2026, 9, 4, 12), "C3"),
            ("T4", 50, datetime(2026, 9, 4, 13), "C4"),
        ],
        ["transaction_id", "amount", "event_time", "customer_id"],
    )
    from src.pipelines.dwd_transaction import clean

    assert clean(df).count() == 2   # -1 和 0 被过滤，剩 100 和 50


def test_enrich_customer_join(spark):
    """DWD enrich_customer：JOIN 后应该带上 customer_level。"""
    txns = spark.createDataFrame(
        [("T1", 100, "C1", "M1")],
        ["transaction_id", "amount", "customer_id", "merchant_id"],
    )
    dim = spark.createDataFrame(
        [("C1", "V1", "北京", datetime(2025, 1, 1))],
        ["customer_id", "level", "region", "register_time"],
    )
    from src.pipelines.dwd_transaction import enrich_customer

    result = enrich_customer(txns, dim)
    assert result.select("customer_level").first()["customer_level"] == "V1"


def test_dws_customer_aggregate_grouping(spark):
    """DWS aggregate：同一客户同一天的多笔交易聚成一行。"""
    from src.pipelines.dws_customer_daily import aggregate

    df = spark.createDataFrame(
        [
            ("C1", "2026-09-04", 100.0, "PAYMENT"),
            ("C1", "2026-09-04", 200.0, "PAYMENT"),
        ],
        ["customer_id", "dt", "amount", "transaction_type"],
    )
    result = aggregate(df)
    assert result.count() == 1
    row = result.first()
    assert row["txn_count"] == 2
    assert row["pay_count"] == 2
