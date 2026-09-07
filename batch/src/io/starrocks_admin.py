"""StarRocks DDL 管理：建库 / 执行 .sql 建表脚本（走 FE MySQL 协议端口 9030）。

为什么需要：StarRocks Spark Connector 只写数据、不会自动建表，
目标表必须先通过 DDL 建好。DDL 文件统一放在仓库根目录 starrocks/ddl/。

用法：
    # 命令行：执行整个 ddl 目录（或单个 .sql 文件）
    python -m src.io.starrocks_admin ../starrocks/ddl

    # 代码里：pipeline 写入前幂等确保表存在
    from src.io.starrocks_admin import ensure_database, apply_ddl_file
    ensure_database()
    apply_ddl_file("../starrocks/ddl/ods_transaction.sql")

所有 DDL 都应写 CREATE TABLE IF NOT EXISTS，可重复执行（幂等）。
"""
import sys
from pathlib import Path

import pymysql
from pymysql.constants import CLIENT

from config.settings import settings

# DDL 目录：batch/src/io/starrocks_admin.py → 向上 3 级到仓库根 → starrocks/ddl
DDL_DIR = Path(__file__).resolve().parents[3] / "starrocks" / "ddl"


def get_connection(database: str | None = None):
    """连 FE 的 MySQL 协议端口（9030）。database=None 时连库外（用于 CREATE DATABASE）。

    MULTI_STATEMENTS：允许一次 execute 发多条分号分隔的 SQL（整文件直接执行）。
    """
    return pymysql.connect(
        host=settings.starrocks_host,
        port=settings.starrocks_query_port,
        user=settings.starrocks_user,
        password=settings.starrocks_password,
        database=database,
        charset="utf8mb4",
        autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS,
    )


def execute_sql(sql: str, database: str | None = None) -> None:
    """执行 SQL：单条或多条（分号分隔）都行；-- 注释由 StarRocks 服务端解析，无需处理。"""
    sql = sql.strip()
    if not sql:
        return
    conn = get_connection(database)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            while cur.nextset():   # 消费多语句的所有结果集
                pass
    finally:
        conn.close()


def ensure_database() -> None:
    """建库（IF NOT EXISTS，幂等），后续 DDL 都在这个库下执行。"""
    execute_sql(
        f"CREATE DATABASE IF NOT EXISTS {settings.starrocks_database}"
    )
    print(f"数据库 {settings.starrocks_database} 已就绪")


def ensure_table(table: str) -> None:
    """写入前调用：确保库存在 + 目标表已按 DDL 建好（幂等）。

    约定 DDL 文件名 = 表名：starrocks/ddl/{table}.sql
    表已存在时 IF NOT EXISTS 直接跳过；DDL 文件缺失则明确报错（提醒先补建表脚本）。
    """
    ensure_database()
    ddl_file = DDL_DIR / f"{table}.sql"
    if not ddl_file.exists():
        raise FileNotFoundError(
            f"目标表 {table} 的 DDL 文件不存在：{ddl_file}\n"
            f"请先在 starrocks/ddl/ 下补 {table}.sql（CREATE TABLE IF NOT EXISTS）"
        )
    apply_ddl_file(ddl_file)


def apply_ddl_file(sql_path: str | Path) -> None:
    """执行一个 .sql 文件：原文（含 -- 注释、多条语句）一次性发给 StarRocks 执行。"""
    sql_path = Path(sql_path)
    execute_sql(sql_path.read_text(encoding="utf-8"),
                database=settings.starrocks_database)
    print(f"DDL 已执行：{sql_path.name}")


def apply_ddl_dir(ddl_dir: str | Path) -> None:
    """执行目录下所有 .sql（文件名排序，保证表按依赖顺序建）。"""
    for sql_file in sorted(Path(ddl_dir).glob("*.sql")):
        apply_ddl_file(sql_file)


if __name__ == "__main__":
    # python -m src.io.starrocks_admin [ddl 文件或目录，默认 ../starrocks/ddl]
    target = sys.argv[1] if len(sys.argv) > 1 else "../starrocks/ddl"
    ensure_database()
    p = Path(target)
    if p.is_dir():
        apply_ddl_dir(p)
    else:
        apply_ddl_file(p)
    print("全部 DDL 执行完成")
