"""每日维度演进入口（Airflow 调度）：新增 / 更新 / 软删 customer 和 merchant。

用法：
    python3 daily_evolve.py 2026-09-13 \
        --add-customer 20 --update-customer 50 --delete-customer 10 \
        --add-merchant 5 --update-merchant 10 --delete-merchant 3

幂等：演进前自动快照到 history/dt={ds}/，重跑同一天先恢复快照再用 seed(ds) 重演。
"""
import argparse
import sys
from pathlib import Path

# generator/ 在项目根下，把项目根加进 sys.path，shared 包才能被 import
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.log import setup_logging, get_logger   # noqa: E402  (path 处理后才能 import)

setup_logging()
logger = get_logger(__name__)

from build_dimensions import evolve_for_day   # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="每日维度演进：新增/更新/软删 customer、merchant")
    parser.add_argument("ds", help="业务日期，格式 YYYY-MM-DD（Airflow 传 {{ ds }}）")
    parser.add_argument("--add-customer", type=int, default=20)
    parser.add_argument("--update-customer", type=int, default=50)
    parser.add_argument("--delete-customer", type=int, default=10)
    parser.add_argument("--add-merchant", type=int, default=5)
    parser.add_argument("--update-merchant", type=int, default=10)
    parser.add_argument("--delete-merchant", type=int, default=3)
    args = parser.parse_args()

    evolve_for_day(
        args.ds,
        n_add_customer=args.add_customer,
        n_update_customer=args.update_customer,
        n_delete_customer=args.delete_customer,
        n_add_merchant=args.add_merchant,
        n_update_merchant=args.update_merchant,
        n_delete_merchant=args.delete_merchant,
    )


if __name__ == "__main__":
    main()
