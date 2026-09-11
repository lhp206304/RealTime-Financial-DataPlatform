"""api 配置：统一从环境变量读取（12-Factor 配置外置）。

命名按「基础设施资源」而非消费方：MinIO 就叫 MINIO_*，ClickHouse 就叫 CLICKHOUSE_*，
全项目（compose / batch / api）共用同一套变量名，由 docker-compose 的 environment 注入。

api 只在容器内跑，连接信息（host/user/password/database）无默认值——
环境变量缺失时 BaseSettings 启动直接报错，强制 deploy/.env 提供真实值。
query_timeout 是代码级行为参数，保留默认值（与部署环境无关）。
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ---- StarRocks（实时链路，MySQL 协议 9030，SQLAlchemy + PyMySQL）----
    starrocks_host: str
    starrocks_mysql_port: int
    starrocks_user: str
    starrocks_password: str
    starrocks_database: str
    starrocks_query_timeout: int = 5      # 单条查询超时秒数（代码级行为参数，非连接信息）

    # ---- ClickHouse（离线链路，HTTP 协议 8123，clickhouse-connect）----
    clickhouse_host: str
    clickhouse_http_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str
    clickhouse_query_timeout: int = 5     # 同上


settings = Settings()
