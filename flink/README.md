# flink —— 实时计算作业

消费 Kafka `transaction` topic，做实时清洗 + 窗口聚合，写入 StarRocks。
**这是整个项目含金量最高的模块。**

---

## 数据流

```text
Kafka(transaction)
   ↓
清洗（空值/异常金额/重复/类型/时间字段）
   ↓
Event Time + Watermark（处理乱序）
   ↓
窗口聚合（5 分钟指标）
   ↓
StarRocks
```

---

## 两种写法（本项目都保留目录）

| 目录 | 方式 | 定位 |
|---|---|---|
| `sql/` | **Flink SQL** | 先跑通链路，贴近数仓 SQL 思维 |
| `jobs/` | **PyFlink DataStream API** | 后续重写关键作业，真正练 State/Watermark |

建议顺序：**先用 `sql/` 跑通 → 再用 `jobs/` 练 State**。

---

## 你要实现的清单

### `sql/`（先做）
- [ ] Kafka Source 表（connector=kafka，format=json）
- [ ] 清洗逻辑（过滤空值/非法金额，`WHERE` 或 `CASE`）
- [ ] 5 分钟窗口聚合：交易笔数 / 总金额 / 平均金额 / 最大金额 / 去重客户数 / 去重商户数
- [ ] StarRocks Sink 表

### `jobs/`（后做，练 State）
- [ ] Kafka Source + Watermark 策略（允许 N 秒乱序）
- [ ] KeyedStream by `customer_id`
- [ ] 用 `ValueState` / `MapState` 算：用户过去 10 分钟交易金额、30 分钟交易次数
- [ ] State TTL 配置
- [ ] Checkpoint 配置

---

## 要练/要能讲清楚的知识点

| 主题 | 面试要能回答 |
|---|---|
| Watermark | 乱序数据怎么处理？迟到数据怎么办？ |
| Window | 滚动 vs 滑动 vs 会话窗口，各用在哪？ |
| State | ValueState/MapState 区别？TTL 为什么要设？ |
| Checkpoint | Exactly-once 怎么保证？Kafka offset 怎么配合？ |
| Parallelism | Flink 并行度和 Kafka partition 怎么匹配？ |
