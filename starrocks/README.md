# starrocks —— OLAP 建表 + 数据导入

作为核心分析存储（实时数仓 OLAP 层），承接 Flink 写入，供 API 查询。

---

## 数仓分层（沿用传统数仓经验）

```text
Flink → ODS → DWD → DWS → ADS
```

| 层 | V1 需要的表 | 说明 |
|---|---|---|
| DWD | `dwd_transaction` | 清洗后的交易明细 |
| DWS | `dws_customer_transaction` | 客户维度聚合 |
| ADS | `ads_realtime_transaction` | 实时大盘指标（给 API/BI） |

> V1 先从 `dwd_transaction` + 一张 ADS 聚合表起步，跑通即可。

---

## 你要实现的清单

### `ddl/`
- [ ] `dwd_transaction` 建表
  - 选 **Key 模型**（Primary Key / Duplicate Key / Aggregate Key，想清楚为什么）
  - 设计 **Partition**（按日期？）、**Bucket/Distribution**（按什么分桶？）、**Sort Key**
- [ ] ADS 聚合表建表
- [ ] （可选）Materialized View 优化查询

### `load/`
- [ ] 数据导入方式配置：**Stream Load** 或 **Routine Load**（直接消费 Kafka）
- [ ] 说明选哪种、为什么

---

## 要练/要能讲清楚的知识点（简历价值最高的部分）

| 主题 | 面试要能回答 |
|---|---|
| Key 模型 | 这张表为什么用 Primary Key / Aggregate Key？ |
| 分桶 | 为什么这样分桶？分桶数怎么定？ |
| Partition | 分区键怎么选？对查询和导入的影响？ |
| Materialized View | 什么场景用 MV 加速？ |
| Query Profile | 查询变慢怎么用 Query Profile 定位？ |
| Routine Load | 和 Stream Load 的区别、各自适用场景？ |
