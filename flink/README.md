# flink —— 实时计算作业

消费 Kafka `transaction` topic，做实时清洗 + 维表打宽 + 窗口聚合，写入 StarRocks。

---

## 数据流

```text
Kafka(transaction)
   ↓
Job1：清洗（空值/异常金额/重复/类型/时间字段）+ Event Time / Watermark（处理乱序）
   ├─ 查 Redis 维表打宽 → StarRocks（dwd_transaction_online，实时明细）
   ├─ 迟到数据（event_time < watermark）→ StarRocks（late_transaction）
   └─ 打宽后明细 → Kafka(dwd_transaction topic)
   ↓
Job2：窗口聚合（TUMBLE 滚动 / HOP 滑动）→ StarRocks（dws_realtime_agg）
```

---

## 目录

| 路径 | 内容 |
|---|---|
| [sql/kafka_to_starrocks.sql](sql/kafka_to_starrocks.sql) | Job1：Kafka Source → 清洗 + Redis 维表打宽（UDF）→ StarRocks 明细 + Kafka DWD topic + 迟到侧输出 |
| [sql/job2.sql](sql/job2.sql) | Job2：消费 DWD topic，TUMBLE/HOP 窗口聚合并写入 `dws_realtime_agg` |
| [udf/](udf/) | `RedisHashLookupFunction`：Flink SQL 查 Redis 维表的自定义函数（Java，Maven 构建后 jar 放 `/opt/flink/lib`） |

作业以 Flink SQL 为主（`sql/`），维表关联通过 [udf/](udf/) 里的 Java UDF 完成。

---

## 关键配置约定

| 项 | 约定 |
|---|---|
| Event Time | `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`（容忍 5 秒乱序） |
| 并行度 | 提交前 `SET 'parallelism.default' = '3'`，与 Kafka topic 3 分区对齐 |
| 建表 | 全部 `CREATE TABLE IF NOT EXISTS`，同 session 重复提交不报错 |
| Connector | Kafka Source/Sink 用 `'connector' = 'kafka'`；StarRocks Sink 用 `'connector' = 'starrocks'` |
| 镜像 | `flink:1.20-java17`，Kafka / StarRocks connector jar 直接放 `/opt/flink/lib` |

> Flink SQL Client 的临时对象（函数/视图）随 `sql-client.sh` 进程生命周期销毁，session 结束即失效。

---

## 设计要点

| 主题 | 说明 |
|---|---|
| 语义保障 | Kafka 至少一次 + StarRocks 主键模型按主键 upsert：checkpoint 恢复后重放的消息覆盖旧行，不产生重复，达到幂等效果 |
| Event Time / Watermark | `event_time - INTERVAL '5' SECOND`：以事件时间驱动窗口，容忍 5 秒乱序；窗口在 watermark 越过窗口结束时间后触发 |
| 迟到数据 | `event_time < CURRENT_WATERMARK` 的记录不进主流，侧输出到 `late_transaction` 表，便于事后核对/补数 |
| 维表打宽 | 不走 Flink State，而是 Redis Lookup（`redis_hash` UDF 查 `dim:customer:` / `dim:merchant:` Hash）：维表由 batch T+1 同步，Flink 不持有维表状态，无状态膨胀 |
| 去重 | Sink 均声明主键（dwd 明细 `(transaction_id, event_time, customer_id)`、聚合 `(window_type, window_start, window_end, customer_id)`）；聚合表 StarRocks 侧是主键模型，重放消息按主键 upsert 覆盖 |
| 窗口复用 | TUMBLE（1 分钟滚动）和 HOP（滑动）结果写同一张 `dws_realtime_agg`，用 `window_type` 列区分，少维护一张表 |
