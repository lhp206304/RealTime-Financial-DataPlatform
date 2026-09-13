"""ClickHouse DDL 管理：建库 / 执行 .sql 建表脚本。

用法：
    # 命令行：执行整个 ddl 目录
    python -m src.io.clickhouse_admin ../clickhouse/ddl

    # 代码里：写入前幂等确保表存在
    from src.io.clickhouse_admin import ensure_table
    ensure_table("ods_transaction")
"""
import sys
from pathlib import Path

import clickhouse_connect

from config.settings import settings
from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

DDL_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "ddl"


def get_client():
    """返回 clickhouse-connect 客户端（HTTP 8123 端口）。"""
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_http_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def ensure_database() -> None:
    """建库（幂等）。"""
    client = get_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
    logger.info("数据库已就绪", database=settings.clickhouse_database)


def ensure_table(table: str) -> None:
    """写入前调用：确保库和目标表已建好（幂等）。
    约定 DDL 文件名 = 表名：clickhouse/ddl/{table}.sql
    """
    ensure_database()
    ddl_file = DDL_DIR / f"{table}.sql"
    if not ddl_file.exists():
        raise FileNotFoundError(
            f"目标表 {table} 的 DDL 文件不存在：{ddl_file}\n"
            f"请先在 clickhouse/ddl/ 下补 {table}.sql"
        )
    apply_ddl_file(ddl_file)


def apply_ddl_file(sql_path: str | Path) -> None:
    """执行一个 .sql 文件（按分号切分逐条执行，ClickHouse 不支持多语句一次发）。"""
    sql_path = Path(sql_path)
    client = get_client()
    sql = sql_path.read_text(encoding="utf-8")
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt and not stmt.startswith("--"):
            client.command(stmt)
    logger.info("DDL 已执行", file=sql_path.name)


def apply_ddl_dir(ddl_dir: str | Path) -> None:
    """执行目录下所有 .sql（按文件名排序）。"""
    for sql_file in sorted(Path(ddl_dir).glob("*.sql")):
        apply_ddl_file(sql_file)


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else str(DDL_DIR)
    ensure_database()
    p = Path(target)
    if p.is_dir():
        apply_ddl_dir(p)
    else:
        apply_ddl_file(p)
    logger.info("全部 DDL 执行完成")