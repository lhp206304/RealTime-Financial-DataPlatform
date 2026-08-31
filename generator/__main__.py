"""主入口：持续造数 → 发到 Kafka。

跑法：在 generator/ 上层目录执行  python -m generator
按 Ctrl+C 优雅退出（会把缓冲区消息发完再停）。
"""

import logging
import time
import random
from datetime import datetime, timedelta, timezone

from .model import make_transaction
from .producer import TransactionProducer

# --- 配置：先写死，步骤 2 后期可改成从环境变量读 ---
BOOTSTRAP_SERVERS = "localhost:9092"   # 宿主机连 Kafka 用 9092
TOPIC = "transaction"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("generator")


def main() -> None:
    producer = TransactionProducer(BOOTSTRAP_SERVERS, TOPIC)
    base = datetime.now(timezone.utc)

    try:
        # TODO 1: 写主循环（while True 持续造数）
        #   循环体里：
        #   a) base += timedelta(seconds=2)         # 事件时间基准往前走
        #   b) txn = make_transaction(base)         # 造一条
        #   c) 取 key 和 value：
        #        key = txn.customer_id              # 按用户分区，同一用户有序
        #        value = txn.model_dump_json()      # 转 JSON 字符串
        #   d) producer.send(key, value)
        #   e) time.sleep(random.uniform(0.5, 2))   # 真实发送节奏（记得 import random）
        while True:
            base+=timedelta(seconds=2)
            txn=make_transaction(base)
            key=txn.customer_id
            value=txn.model_dump_json()
            producer.send(key,value)
            time.sleep(random.uniform(0.5,2))
    except KeyboardInterrupt:
        # TODO 2: 收到 Ctrl+C，打一条日志说要退出了
        #   提示：logger.info("shutting down...")
        logger.info("keyboard interrupt, shutting down...")
    finally:
        # TODO 3: 无论如何都要 producer.close()，把没发完的消息 flush 出去
        producer.close()
        logger.info("producer closed")


if __name__ == "__main__":
    main()
