# generator —— 数据生成模块（维度表 + 交易流水 + Kafka 实时流）

用 Python 生成模拟金融数据：三张维度表和交易流水落地 MinIO（Parquet），实时交易流发送到 Kafka `transaction` topic。

> Pydantic 做 Schema 校验；交易 ID 全部来自维度池，保证下游 JOIN 不落空。

---

## 文件职责

| 文件 | 干什么 |
|---|---|
| [schema.py](schema.py) | 表结构定义：事实表 `Transaction` + 三张维度表（Pydantic 模型 + 枚举） |
| [build_dimensions.py](build_dimensions.py) | 造三张维度表（客户/账户/商户）→ Parquet → MinIO `dim` 桶 |
| [build_transactions.py](build_transactions.py) | 造交易流水 → Parquet → MinIO `fact` 桶（批量模式）；也提供实时造单条函数 |
| [kafka_producer.py](kafka_producer.py) | Kafka Producer 封装（acks=all、重试、回调日志、flush） |
| [send_realtime.py](send_realtime.py) | 实时流入口：造一条 → 发 Kafka，Ctrl+C 优雅退出 |

---

## 数据流

```text
schema.py 定义表结构
    ↓
build_dimensions.py ──→ MinIO dim 桶（dim_customer / dim_account / dim_merchant.parquet）
    ↓ 读维度池
build_transactions.py ──→ MinIO fact 桶（fact_transaction.parquet，批量补历史）
    ↓
send_realtime.py ──→ Kafka transaction topic（实时流，喂 Flink）
```

两种生成模式（`GenMode`）：

| 模式 | event_time | 用途 |
|---|---|---|
| `batch` | 开户时间 + 0~120 小时随机 | 离线补历史数据 |
| `realtime` | 当前时间往前 0~5 秒（乱序） | 实时流，给 Flink Watermark |

---

## 快速开始

```bash
# 0. 起依赖（Kafka 3 分区 topic 由 init-kafka 自动创建；MinIO 需在跑）
docker compose -f ../deploy/docker-compose.yml up -d

# 1. 造维度表 → MinIO dim 桶（首次用 override=True 全量造）
python build_dimensions.py

# 2a. 批量造交易 → MinIO fact 桶
python build_transactions.py

# 2b. 实时流 → Kafka（一直发，Ctrl+C 退出）
python send_realtime.py
```

依赖安装：`pip install -r requirements.txt`

---

## 交易数据样例

```json
{
  "transaction_id": "T1c67f76c2e384096",
  "amount": "7833.49",
  "currency": "EUR",
  "customer_id": "C16117",
  "account_id": "A28275",
  "merchant_id": "M10030",
  "transaction_type": "REFUND",
  "event_time": "2026-08-07T05:33:07.826Z"
}
```

要点：金额用 Decimal（JSON 里是字符串，避免浮点误差）；`event_time` 截到毫秒（Flink JSON 只认 3 位微秒）；同一 `customer_id` 作为 Kafka key 进同一分区，保证单客户有序。

---

## 设计要点

| 主题 | 说明 |
|---|---|
| Partition | Producer key 选 `customer_id`：同一客户的消息进同一分区，Flink keyBy 后单客户事件保序 |
| Delivery Semantics | `acks=all`（ISR 全部确认）+ `retries=5` 保证金融数据可靠投递；语义是 at-least-once，重复由下游 StarRocks 主键 upsert 幂等兜底 |
| Pydantic | 入 Kafka 前做 Schema 校验（金额/枚举/ID 完整性），脏数据在源头拦下，不进流 |
| 乱序与 Watermark | realtime 模式造 0~5 秒乱序，对应 Flink `WATERMARK ... - INTERVAL '5' SECOND` 的容忍窗口；超过窗口的迟到数据进 late 表 |
| MinIO 追加写 | `put_object` 是整对象覆盖写，追加历史数据采用「读旧 Parquet → concat 合并 → 整体写回」 |
