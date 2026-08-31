"""Kafka Producer 封装：只管「怎么把一条消息发出去」。"""

from email import message
import logging

from confluent_kafka import Producer, KafkaError, Message

logger = logging.getLogger(__name__)


def _on_delivery(err: KafkaError|None, msg: Message):
    """发送结果回调。produce() 是异步的，发成功/失败后 Kafka 回调这里。

    err 不为 None → 发送失败；否则成功。
    """
    if err is not None:
        # TODO: 打一条 error 日志，把 err 和消息的 key 记下来
        #   提示：logger.error(...)；msg.key() 拿到 key（bytes，需 .decode()）
        logger.error(f"Failed to deliver message:{msg.key().decode()} with error:{err}")
    else:
        # TODO: 打一条 debug/info 日志：发到了哪个 topic、哪个 partition、offset
        #   提示：msg.topic() / msg.partition() / msg.offset()
        logger.info(f"Delivered info: topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} message={msg.value()}")


class TransactionProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer= Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks":"all",
            "retries":3
            })
        # TODO 1: 创建 confluent_kafka.Producer
        #   配置字典至少要有 "bootstrap.servers": bootstrap_servers
        #   （失败重试：confluent-kafka 默认会重试，可选加 "retries"/"acks":"all"）
        #   self.producer = Producer({...})

    def send(self, key: str, value: str) -> None:
        """发一条消息。key 决定 partition，value 是 JSON 字符串。"""
        # TODO 2: 调 self.producer.produce(...)
        #   参数：topic=self.topic, key=key, value=value, callback=_on_delivery
        #   注意：key/value 传 str 即可，库会自动编码成 bytes
        # TODO 3: 调 self.producer.poll(0)
        #   作用：触发已完成消息的回调（不阻塞）。不调回调不会执行
        self.producer.produce(topic=self.topic, key=key, value=value, callback=_on_delivery)
        self.producer.poll(0)

    def close(self) -> None:
        """退出前调用：把缓冲区里没发完的消息全部发出去。"""
        # TODO 4: 调 self.producer.flush()
        #   不调的话，缓冲里的消息可能还没发程序就退了 → 丢消息
        self.producer.flush()
