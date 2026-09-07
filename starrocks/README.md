# starrocks —— 实时链路 OLAP 建表

实时数仓 OLAP 层，承接 Flink 写入，供 API 查询实时指标。

> 离线批链路（PySpark T+1）的表在 ClickHouse，DDL 见 [../clickhouse/ddl/](../clickhouse/ddl/)。

---

## 表清单（ddl/）

| DDL | 表 | 写入方 | 说明 |
|---|---|---|---|
| [dws_realtime_agg.sql](ddl/dws_realtime_agg.sql) | `finance.dws_realtime_agg` | Flink Job2（窗口聚合） | 客户维度实时聚合，Primary Key `(window_type, window_start, window_end, customer_id)`，TUMBLE/HOP 两种窗口结果 upsert 进同一张表 |
| [late_transaction.sql](ddl/late_transaction.sql) | `finance.late_transaction` | Flink Job1（清洗） | 迟到交易明细（event_time < watermark），Duplicate Key 明细模型，按客户维度排查迟到原因 |

实时明细表 `dwd_transaction_online` 由 Flink SQL 作业里的 Sink 连接器定义（见 [../flink/sql/kafka_to_starrocks.sql](../flink/sql/kafka_to_starrocks.sql)），不在本目录。

---

## 数据导入方式

Flink 通过 StarRocks Connector（JDBC + Stream Load）写入：Sink 表在 Flink SQL 里声明 `'connector' = 'starrocks'`，主键模型表按主键 upsert，作业重跑/恢复不产生重复行。

建表（首次部署或表结构变更时执行）：

```bash
# 逐个执行 ddl/ 下的脚本（FE MySQL 协议端口 9030）
mysql -h 127.0.0.1 -P 9030 -u root < ddl/dws_realtime_agg.sql
mysql -h 127.0.0.1 -P 9030 -u root < ddl/late_transaction.sql
```

---

## 设计要点

| 主题 | 说明 |
|---|---|
| Key 模型 | 两张表都用 Primary Key：Flink 持续 upsert 聚合结果/明细，按主键覆盖天然幂等；分区列必须进主键（如 `(window_type, window_start, window_end, customer_id)`） |
| 分区 | `date_trunc('day', ...)` 表达式按天分区，与 Flink 微批/重跑周期对齐，查询按天裁剪 |
| 分桶 | 聚合表按 `customer_id`、迟到明细按 `transaction_id` HASH 分桶，与高频查询的过滤/JOIN 键一致 |
| 导入方式 | 走 Flink StarRocks Connector（底层 Stream Load），而不是 Routine Load 直接消费 Kafka：清洗、打宽、窗口聚合逻辑在 Flink 层，StarRocks 只接收成品结果 |
| 物化视图 | 未使用 MV：聚合在 Flink 层预计算完成，StarRocks 侧只做点查和简单过滤，避免双引擎各算一套口径 |
