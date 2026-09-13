"""每日流水造数入口（Airflow 调度）：用 ACTIVE 维度池造 ds 当天交易，幂等写回 fact 桶。

用法：
    python3 daily_txn.py 2026-09-13 --count 5000

幂等：写回前剔除 fact_transaction.parquet 里 event_time 属于 ds 的旧行，
配合 seed(ds)，Airflow 重试 / 手动重跑同一天结果一致、不重复。
必须在 daily_evolve 之后跑（新交易只引用演进后的 ACTIVE 实体）。
"""
import argparse
import sys
from pathlib import Path

# generator/ 在项目根下，把项目根加进 sys.path，shared 包才能被 import
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.log import setup_logging, get_logger   # noqa: E402  (path 处理后才能 import)

setup_logging()
logger = get_logger(__name__)

from build_transactions import generate_for_day   # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="每日交易流水造数（event_time 落在 ds 当天）")
    parser.add_argument("ds", help="业务日期，格式 YYYY-MM-DD（Airflow 传 {{ ds }}）")
    parser.add_argument("--count", type=int, default=5000, help="当天造数条数")
    args = parser.parse_args()

    generate_for_day(args.ds, args.count)


if __name__ == "__main__":
    main()
