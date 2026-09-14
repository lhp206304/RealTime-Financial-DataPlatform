# generator —— 数据生成模块（维度表 + 交易流水 + Kafka 实时流 + 每日演进）

用 Python 生成模拟金融数据：三张维度表和交易流水落地 MinIO（Parquet），实时交易流发送到 Kafka `transaction` topic。支持每日维度演进（新增/更新/软删）和当日流水生成，由 Airflow 调度。

> Pydantic 做 Schema 校验；交易 ID 全部来自维度池，保证下游 JOIN 不落空。

---

## 文件职责

| 文件 | 干什么 |
|---|---|
| [schema.py](schema.py) | 表结构定义：事实表 `Transaction` + 三张维度表（Pydantic 模型 + 枚举），含 `DimStatus(ACTIVE/DELETED)` |
| [config.py](config.py) | 连接配置：MinIO 地址/密钥/桶名全部从环境变量读，无默认值，缺失即报错 |
| [build_dimensions.py](build_dimensions.py) | 造三张维度表（客户/账户/商户）→ Parquet → MinIO `dim` 桶；支持每日演进（新增/更新/软删）+ 快照备份/恢复 |
| [build_transactions.py](build_transactions.py) | 造交易流水 → Parquet → MinIO `fact` 桶（批量模式 + 当日模式）；当日模式只引用 ACTIVE 维度实体 |
| [daily_evolve.py](daily_evolve.py) | 每日演进入口：`python daily_evolve.py {ds} --add-customer N ...`，Airflow 调用 |
| [daily_txn.py](daily_txn.py) | 每日造流水入口：`python daily_txn.py {ds} --count N`，Airflow 调用 |
| [kafka_producer.py](kafka_producer.py) | Kafka Producer 封装（acks=all、重试、回调日志、flush） |
| [send_realtime.py](send_realtime.py) | 实时流入口：造一条 → 发 Kafka，Ctrl+C 优雅退出 |

---

## 数据流

```text
schema.py 定义表结构（含 status 字段）
    ↓
build_dimensions.py ──→ MinIO master 桶（dim_customer / dim_account / dim_merchant.parquet）
    │                      │
    │                      └─ 软删除（status=DELETED）：不删行，只改状态
    │                         历史流水行仍在，DWD LEFT JOIN 不落空
    ↓ 读维度池（只收 ACTIVE）
build_transactions.py ──→ MinIO transaction 桶（fact_transaction.parquet）
    │   · 批量模式：补历史数据
    │   · 当日模式：event_time 落在指定日期，seed(ds) 保证幂等
    ↓
send_realtime.py ──→ Kafka transaction topic（实时流，喂 Flink）
```

每日演进（Airflow 调度）：

```text
daily_evolve.py {ds} ──→ 快照备份 → 新增/更新/软删维度 → 写回 MinIO
daily_txn.py {ds}      ──→ 只引用 ACTIVE 实体造当天流水 → 写回 MinIO（剔除同 ds 旧行）
```

桶命名约定（MinIO 是数仓**上游**，按上游数据语义命名，不用数仓建模概念）：

| 桶 | 装什么 | 桶名变量 |
|---|---|---|
| `master` | 主数据：customer / account / merchant（变化慢，有软删状态） | `MINIO_BUCKET_MASTER` |
| `transaction` | 业务流水：fact_transaction（事件流） | `MINIO_BUCKET_TRANSACTION` |

> 桶里的 Parquet 文件名仍叫 `dim_customer.parquet` / `fact_transaction.parquet`——那是下游数仓的表名；桶按数据来源分类，表名保持数仓口径，两层概念不混。

两种生成模式（`GenMode`）：

| 模式 | event_time | 用途 |
|---|---|---|
| `batch` | 开户时间 + 0~120 小时随机 | 离线补历史数据 |
| `realtime` | 当前时间往前 0~5 秒（乱序） | 实时流，给 Flink Watermark |

---

## 快速开始

### 首次全量造数

```bash
# 0. 起依赖（Kafka 3 分区 topic 由 init-kafka 自动创建；MinIO 需在跑）
docker compose -f ../deploy/docker-compose.yml up -d

# 1. 造主数据 → MinIO master 桶
python build_dimensions.py

# 2a. 批量造交易 → MinIO transaction 桶
python build_transactions.py

# 2b. 实时流 → Kafka（一直发，Ctrl+C 退出）
python send_realtime.py
```

依赖安装：`pip install -r requirements.txt`

### 每日演进（Airflow 自动调度）

```bash
# 演进维度（新增 20 客户 / 更新 50 / 软删 10，商户同理）
python daily_evolve.py 2026-09-13 --add-customer 20 --update-customer 50 --delete-customer 10 \
    --add-merchant 5 --update-merchant 10 --delete-merchant 3

# 造当天流水（5000 条，event_time 落在当天）
python daily_txn.py 2026-09-13 --count 5000
```

> 幂等保证：`seed(ds)` 使同一天重跑结果一致；快照备份到 master 桶 `history/dt={ds}/`，重跑先恢复再演进；流水写回前剔除同 ds 旧行。

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
| 软删除 | customer/merchant 加 `status` 字段（ACTIVE/DELETED），删除不删行只改状态；新流水只引用 ACTIVE 实体，历史流水 JOIN 不落空 |
| 快照幂等 | 每日演进前把当前 parquet 备份到 master 桶 `history/dt={ds}/`，重跑同一天先恢复快照再演进；流水写回前剔除同 ds 旧行，`seed(ds)` 保证可重复 |
