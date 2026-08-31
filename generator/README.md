# generator —— 数据生成 + Kafka Producer (Python)

用 Python 生成模拟金融交易数据，通过 Kafka Producer 发送到 `transaction` topic。

> Data Engineer 主链路组件。用 Pydantic 做 Schema 校验，体现「Python Data Engineering」而非只会 Pandas。

---

## 目录布局

```text
generator/
├── generator/          # 包代码（你写）
│   ├── __init__.py
│   ├── model.py        # Pydantic 模型
│   ├── producer.py     # Kafka Producer 封装
│   └── __main__.py     # 入口：python -m generator
├── tests/              # pytest（你写）
├── pyproject.toml      # 依赖/配置
└── README.md
```

---

## 数据实体

V1 只需要 `Transaction`（其他实体后续阶段再加）：

```json
{
  "transaction_id": "TX100001",
  "customer_id": 10001,
  "account_id": 20001,
  "merchant_id": 30001,
  "amount": 1288.50,
  "currency": "CNY",
  "transaction_type": "PAYMENT",
  "event_time": "2026-08-30T18:00:01"
}
```

---

## 你要实现的清单

### `model.py`
- [ ] 用 **Pydantic** 定义 `Transaction`（字段类型 + 校验：金额>0、币种枚举、时间格式）
- [ ] 造数逻辑：随机 customer/merchant/amount/type，`event_time` 故意制造少量**乱序**（给 Flink Watermark 用）

### `producer.py`
- [ ] 封装 Kafka Producer（推荐 `confluent-kafka`）
- [ ] 按 `customer_id` 或 `account_id` 做 **partition key**（保证同一用户消息有序）
- [ ] 发送失败处理：重试 / 错误日志

### `__main__.py`
- [ ] 从配置读 Kafka 地址、topic、生产速率（如每秒 N 条）
- [ ] 主循环生产 + 速率控制
- [ ] 优雅退出（signal 处理，flush 剩余消息）

---

## 要练/要能讲清楚的知识点

| 主题 | 面试要能回答 |
|---|---|
| Partition | 为什么这么设计 partition？key 怎么选？ |
| Message Ordering | 怎么保证同一用户消息有序？ |
| Delivery Semantics | at-least-once / exactly-once 怎么权衡？ |
| Pydantic | 为什么用 Schema 校验？校验失败怎么处理？ |

---

## 建议实现顺序

1. 先跑通「造一条 → 打印」
2. 再接 Kafka「造一条 → 发一条」
3. 最后上速率控制 + 优雅退出 + 单元测试
