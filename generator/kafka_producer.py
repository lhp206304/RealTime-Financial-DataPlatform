"""Kafka Producer 封装：只管「怎么把一条消息发出去」。"""

from confluent_kafka import Producer, KafkaError, Message

from shared.log import get_logger

logger = get_logger(__name__)


def _on_delivery(err: KafkaError | None, msg: Message):
    """发送结果回调。produce() 是异步的，发成功/失败后 Kafka 回调这里。

    err 不为 None → 发送失败；否则成功。
    """
    if err is not None:
        logger.error("消息发送失败", key=msg.key().decode(), error=str(err))
    else:
        logger.info(
            "发送成功",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


class TransactionProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        # acks=all + 重试：金融数据可靠性要求（ISR 全部确认才算发成功）
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 5,
        })

    def send(self, key: str, value: str) -> None:
        """发一条消息。key 决定 partition，value 是 JSON 字符串。"""
        self.producer.produce(
            topic=self.topic, key=key, value=value, callback=_on_delivery
        )
        # poll(0)：触发已完成消息的回调（不阻塞）；不调回调不会执行
        self.producer.poll(0)

    def close(self) -> None:
        """退出前调用：把缓冲区里没发完的消息全部发出去，避免丢消息。"""
        self.producer.flush()
