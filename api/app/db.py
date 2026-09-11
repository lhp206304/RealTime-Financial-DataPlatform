"""查询层：双数据源。

- StarRocks（MySQL 协议 / SQLAlchemy + PyMySQL 连接池）：
    实时链路表（Flink 写入，如 dwd_transaction_online）→ run_query()
- ClickHouse（HTTP 协议 / clickhouse-connect）：
    离线链路表（PySpark T+1 写入，如 ads_customer_profile）→ run_query_clickhouse()

V3 迁移说明：离线层从 StarRocks 换到 ClickHouse，但实时层仍在 StarRocks，
所以两套查询入口并存；routers 按表的数据来源选对应函数。
"""

import re

import clickhouse_connect
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.settings import settings

# ---- StarRocks 连接配置（实时链路）----
# 全部从环境变量经 settings 读取（见 app/settings.py）
DB_HOST = settings.starrocks_host
DB_PORT = settings.starrocks_mysql_port
DB_USER = settings.starrocks_user
DB_PASSWORD = settings.starrocks_password
DB_NAME = settings.starrocks_database

# 查询超时（秒）：单条 SQL 跑太久就掐断
QUERY_TIMEOUT = settings.starrocks_query_timeout

# ---- ClickHouse 连接配置（离线链路）----
CH_HOST = settings.clickhouse_host
CH_PORT = settings.clickhouse_http_port          # HTTP 端口
CH_USER = settings.clickhouse_user
CH_PASSWORD = settings.clickhouse_password
CH_NAME = settings.clickhouse_database
CH_QUERY_TIMEOUT = settings.clickhouse_query_timeout


# 模块级单例：整个进程共用一个 engine（内含连接池）
_engine: Engine | None = None
# ClickHouse 客户端单例（HTTP 连接轻量，不需要连接池）
_ch_client: clickhouse_connect.driver.client.Client | None = None


def _build_url() -> str:
    """拼 SQLAlchemy 连接串：mysql+pymysql://user:pwd@host:port/db"""
    return (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def init_engine() -> Engine:
    """创建 engine + 连接池。在 FastAPI 启动事件里调用一次。

    连接池参数：
    - pool_size：常驻连接数
    - max_overflow：高峰额外可借的连接数
    - pool_recycle：连接多久回收一次，防被 StarRocks 端断掉的死连接
    - pool_pre_ping：借连接前先 ping 一下，剔除已失效连接
    """
    global _engine
    if _engine is not None:
        return _engine

    _engine = create_engine(
        _build_url(),
        pool_size=5,
        max_overflow=5,
        pool_recycle=3600,
        pool_pre_ping=True,
        # 单次连接 + 查询超时，透传给 PyMySQL
        connect_args={
            "connect_timeout": 5,
            "read_timeout": QUERY_TIMEOUT,
        },
    )
    return _engine


def get_engine() -> Engine:
    """给路由用的依赖：拿到已初始化的 engine。"""
    if _engine is None:
        raise RuntimeError("engine 未初始化，先在启动事件里调 init_engine()")
    return _engine


def dispose_engine() -> None:
    """关闭连接池。在 FastAPI 关闭事件里调用，优雅释放连接。"""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


def run_query(sql: str, params: dict | None = None) -> list[dict]:
    """执行只读查询（StarRocks，实时链路表），返回 list[dict]（每行一个 dict）。

    路由里这样用：run_query("SELECT ... WHERE customer_id=:cid", {"cid": cid})
    注意：用命名参数 :name 防 SQL 注入，不要手动拼字符串。
    """
    # init_engine()
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params or {})
        return result.mappings().all()


# ==================== ClickHouse（离线链路表）====================


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    """拿 ClickHouse 客户端（懒加载单例，首次调用时创建）。"""
    global _ch_client
    if _ch_client is None:
        _ch_client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_NAME,
            # clickhouse-connect 1.x 移除了顶层 query_timeout，超时改走 settings：
            # max_execution_time = ClickHouse 服务端单查询超时（秒）
            settings={"max_execution_time": CH_QUERY_TIMEOUT},
        )
    return _ch_client


def _convert_placeholders(sql: str) -> str:
    """SQLAlchemy 风格 :name 占位符 → clickhouse-connect 风格 %(name)s。

    这样 routers 里的 SQL 不用改写法，StarRocks / ClickHouse 两边通用。
    """
    return re.sub(r":(\w+)", r"%(\1)s", sql)


def run_query_clickhouse(sql: str, params: dict | None = None) -> list[dict]:
    """执行只读查询（ClickHouse，离线链路表），返回 list[dict]（每行一个 dict）。

    用法和 run_query 完全一致（:name 占位符），内部自动转换参数风格。
    查离线表用这个：ads_customer_profile / dws_* 等 PySpark T+1 写入的表。
    """
    if params:
        sql = _convert_placeholders(sql)
    result = get_clickhouse_client().query(sql, params)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]
