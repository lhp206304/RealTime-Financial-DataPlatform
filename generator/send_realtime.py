"""实时交易流：造一条 → 发 Kafka，模拟真实交易流。

与 generate.py 的分工：
    generate.py  批量模式 → 写 MinIO（离线补历史数据）
    stream.py    实时模式 → 发 Kafka（喂给 Flink 实时链路）

用法（需先 docker compose up 起 Kafka，transaction topic 已预建 3 分区）：
    python stream.py
按 Ctrl+C 优雅退出（flush 缓冲区后结束，不丢消息）。
"""
import logging
import random
import time

from build_transactions import load_dimension_pools, make_realtime_transaction
from kafka_producer import TransactionProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"   # 本机连 Kafka（docker 映射 9092）
TOPIC = "transaction"


def stream_to_kafka(pools: dict, producer: TransactionProducer) -> None:
    """无限循环：实时造交易 → 发 Kafka；Ctrl+C 优雅退出。"""
    count = 0
    try:
        while True:
            txn = make_realtime_transaction(pools)
            # key=customer_id：同一客户进同一分区，保证单客户内有序（Flink 按 key JOIN 不乱序）
            producer.send(key=txn.customer_id, value=txn.model_dump_json())
            count += 1
            # 随机歇 0.5~2 秒，模拟真实交易疏密不均
            time.sleep(random.uniform(0.5, 2))
    except KeyboardInterrupt:
        logger.info(f"收到退出信号，共发送 {count} 条")
    finally:
        producer.close()   # 把缓冲区里没发完的消息全部发出去


if __name__ == "__main__":
    pools = load_dimension_pools()
    producer = TransactionProducer(BOOTSTRAP_SERVERS, TOPIC)
    stream_to_kafka(pools, producer)
