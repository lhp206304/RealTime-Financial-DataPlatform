"""StarRocks 连接层（MySQL 协议）。

设计要点：
- StarRocks 兼容 MySQL 协议 → 用 PyMySQL 驱动 + SQLAlchemy 连接池。
- 连接池：进程内复用连接，避免每次请求都握手（贵）。
- 查询超时：慢查询不能拖垮整个服务。
"""

import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---- 连接配置（从环境变量读，给本地默认值）----
# 宿主机跑 API 用 localhost；如果 API 也进容器，改成 service 名 starrocks
DB_HOST = os.getenv("STARROCKS_HOST", "localhost")
DB_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
DB_USER = os.getenv("STARROCKS_USER", "root")
DB_PASSWORD = os.getenv("STARROCKS_PASSWORD", "")
DB_NAME = os.getenv("STARROCKS_DB", "finance")

# 查询超时（秒）：单条 SQL 跑太久就掐断
QUERY_TIMEOUT = int(os.getenv("STARROCKS_QUERY_TIMEOUT", "5"))


# 模块级单例：整个进程共用一个 engine（内含连接池）
_engine: Engine | None = None


def _build_url() -> str:
    """拼 SQLAlchemy 连接串：mysql+pymysql://user:pwd@host:port/db"""
    return (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def init_engine() -> Engine:
    """创建 engine + 连接池。在 FastAPI 启动事件里调用一次。

    连接池参数（面试要能讲）：
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
    """执行只读查询，返回 list[dict]（每行一个 dict）。

    路由里这样用：run_query("SELECT ... WHERE customer_id=:cid", {"cid": cid})
    注意：用命名参数 :name 防 SQL 注入，不要手动拼字符串。

    TODO(你写)：
      1. get_engine().connect() 拿连接（with 语句自动归还池子）
      2. conn.execute(text(sql), params or {})
      3. result.mappings().all() → 转成 list[dict] 返回
    """
    # init_engine()
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params or {})
        return result.mappings().all()
