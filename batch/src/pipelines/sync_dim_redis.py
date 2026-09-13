"""表任务：ClickHouse 维表 → Redis 批量同步（checklist 步骤 7，T+1）。

职责：读 ClickHouse dim_customer_offline / dim_merchant_offline → 批量写 Redis，
      供实时 Flink Lookup Join 打宽使用。
调度：python -m src.pipelines.sync_dim_redis

key 设计：dim:customer:{customer_id} / dim:merchant:{merchant_id}（Hash 结构）
刷新策略：全量覆盖（hset 幂等）；维表实体在 T+1 全量刷新下基本只增不改删，
孤儿 key 清理暂不做，需要时按「SCAN 对比 ID 集合 → DELETE」补。
"""
from shared.log import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import clickhouse_connect
import redis

from config.settings import settings

# 维表 → (Redis key 前缀, 主键列)
DIM_TABLES = {
    "dim_customer_offline": ("dim:customer:", "customer_id"),
    "dim_merchant_offline": ("dim:merchant:", "merchant_id"),
}
BATCH_SIZE = 1000     # pipeline 每 1000 条 execute 一次，防单包过大


def fetch_dim_rows(table: str) -> list[dict]:
    """读 ClickHouse 维表全量（HTTP 8123 直查，不起 Spark）。

    加 FINAL：ReplacingMergeTree 后台 merge 未完成时强制去重。
    """
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_http_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    result = client.query(f"SELECT * FROM {table} FINAL")
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def sync_table(r: redis.Redis, table: str, prefix: str, pk: str) -> int:
    """pipeline 分批 hset 全量覆盖，返回同步行数。

    值处理：None → ""（redis-py mapping 不接受 None）；datetime/Decimal → str。
    """
    rows = fetch_dim_rows(table)
    pipe = r.pipeline(transaction=False)     # 只要省网络往返，不需要事务
    for i, row in enumerate(rows, 1):
        fields = {
            k: ("" if v is None else str(v))
            for k, v in row.items() if k != pk
        }
        pipe.hset(f"{prefix}{row[pk]}", mapping=fields)
        if i % BATCH_SIZE == 0:
            pipe.execute()
    pipe.execute()                           # 收尾不足 1000 的部分
    return len(rows)


def verify(r: redis.Redis, prefix: str, expected: int) -> None:
    """校验：Redis 里该前缀的 key 数 == 维表行数。"""
    n = sum(1 for _ in r.scan_iter(f"{prefix}*"))
    assert n == expected, f"Redis {prefix}* 有 {n} 个 key，维表有 {expected} 行"


def run() -> None:
    r = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,   # 读出来是 str，不是 bytes
    )
    r.ping()                     # 连不通直接失败，别等写完才报错
    for table, (prefix, pk) in DIM_TABLES.items():
        n = sync_table(r, table, prefix, pk)
        verify(r, prefix, n)
        logger.info("Redis 同步完成", table=table, prefix=prefix, rows=n)


if __name__ == "__main__":
    run()
