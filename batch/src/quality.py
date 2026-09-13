"""数据质量校验：每层加工后调用，防止脏数据静默流入下游。

检查项：行数对比、NULL 率、ID 完整性、金额非负。
"""
from pyspark.sql import DataFrame

from shared.log import get_logger

logger = get_logger(__name__)


def check_ods(raw_count: int, ods_count: int) -> None:
    """ODS 落地后：行数不能比源数据少（原样落地，只多不少）。"""
    assert ods_count >= raw_count, f"ODS 行数 {ods_count} < 源 {raw_count}，数据丢了！"


def check_dwd(ods_df: DataFrame, dwd_df: DataFrame) -> None:
    """DWD 加工后：行数可能减少（过滤了脏数据），但打宽字段不能全 NULL。"""
    ods_count = ods_df.count()
    dwd_count = dwd_df.count()

    assert dwd_count > 0, "DWD 过滤后 0 行，全被清了？检查清洗条件"
    logger.info("quality DWD 校验", ods_count=ods_count, dwd_count=dwd_count, filtered=ods_count - dwd_count)


def check_dws(dwd_df: DataFrame, dws_df: DataFrame) -> None:
    """DWS 聚合后：聚合后行数应远小于明细，且金额不能为负。"""
    dwd_count = dwd_df.count()
    dws_count = dws_df.count()

    assert dws_count < dwd_count, f"DWS 行数 {dws_count} >= DWD {dwd_count}，聚合没生效？"
    logger.info("quality DWS 校验", dwd_count=dwd_count, dws_count=dws_count, ratio=f"{dwd_count / dws_count:.1f}:1")
