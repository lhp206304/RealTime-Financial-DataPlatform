# 分阶段路线 (Roadmap)

> 核心原则：**不要一次全做**。一个阶段跑通再进下一个。

---

## V1 —— 实时主链路 【✅ 已完成】

```text
Python Generator → Kafka → Flink → StarRocks → FastAPI
```

目标：端到端跑通。这是第一优先级。
落地清单见 [checklists/v1.md](./checklists/v1.md)。

---

## V2 —— 流批一体（Lambda 架构：实时 + 离线双链路）【✅ 已完成】

```text
离线（批处理层，T+1）：
  MinIO 历史 Parquet → PySpark（ODS→DWD→DWS→ADS）→ ClickHouse
  维表 dim_* → ClickHouse ──T+1 同步──▶ Redis（维表缓存）
                                          ▲ Lookup 打宽
实时（速度层）：                            │
  Kafka(transaction) → Flink Job1(清洗 + 查 Redis 打宽 + 迟到侧输出) → Kafka(dwd_transaction)
                     → Flink Job2(Watermark/Checkpoint/TUMBLE+HOP 窗口) → StarRocks

服务层：FastAPI 双数据源 —— 实时表查 StarRocks、离线表查 ClickHouse
```

两条链路**独立算、分开存**（StarRocks 承接实时、ClickHouse 承接离线），即 Lambda 架构：

1. **批处理层**：MinIO 历史数据 → PySpark 走完整数仓分层（ODS→DWD→DWS→ADS）+ 维度表 → **ClickHouse**；维表 T+1 同步进 Redis
2. **速度层**：Kafka topic 分层（transaction → dwd_transaction）+ **Redis Lookup 打宽** + Watermark/Checkpoint/窗口聚合 → StarRocks；迟到数据侧输出到 late 表
3. **服务层**：API 按表的数据来源路由到对应引擎

> 与原计划的偏差：V2 设计时离线/实时汇聚到**同一个 StarRocks**，实际落地改为离线写 **ClickHouse**——离线大批量写不与实时查询争抢资源，双引擎各取所长（StarRocks 实时 upsert/点查、ClickHouse 离线大扫描），代价是 API 维护双数据源。

新增组件：MinIO（对象存储，本地替代 R2）、Redis（维表缓存）、ClickHouse（离线 OLAP）。
落地清单见 [checklists/v2.md](./checklists/v2.md)，架构原理见 [v2-realtime-warehouse-architecture.md](./knowledge/flink/v2-realtime-warehouse-architecture.md)。

---

## V3 —— 工程化 + Airflow 调度【当前阶段】

1. **Python 工程化**：Pydantic、pytest、logging、配置管理、异常处理、Docker 化（部分已随 V2 落地）
2. **Airflow 调度落地**（离线链路从手工跑批改为定时调度）：
   - 配置说明见 [checklists/v3-airflow.md](./checklists/v3-airflow.md)（部署方式、DAG 定义、环境变量、backfill、重试告警、前置检查）
   - `src/pipelines/` 一个模块 = 一张表 = 一个 task，`run(dt)` 已支持业务日期参数（DAG 里传 `{{ ds }}`），无需改作业代码
   - 依赖 DAG：维表组 `ods_customer/ods_merchant → dim_customer/dim_merchant → sync_dim_redis`；事实链路 `ods_transaction → dwd_transaction → [dws_customer_daily, dws_merchant_daily] → [ads_customer_profile, ads_daily_report, ads_merchant_top10]`（dwd 前需维表组就绪）
   - Airflow 部署（docker-compose 加服务或 standalone），T+1 schedule（如每日凌晨），支持历史回补 backfill
   - 任务级失败重试、告警；每层作业的前置依赖检查（上游当天分区有数据才跑）

---

## V4 —— 数据质量

独立 DQ 模块：NULL / 重复 / 非法金额 / 非法时间 / 缺失 ID / Schema 错误 + 质量报告。

---

## V5 —— AI / 算法

```text
历史数据 → PySpark 特征工程 → Scikit-learn → 风险模型 → Flink 实时风险评分
```

从 Isolation Forest 入手。重点是「数据 → 特征 → 模型 → 评价 → 应用」，不是数学推导。

---

## V6 —— Cloud

Cloudflare R2 + CI/CD (GitHub Actions) + 云部署。最后做。

---

## V7 —— Go 并行实现（高并发）

主链路稳定后，用 **Go 重写 generator 和 API**，作为独立并行实现：

```text
Go Generator → Kafka                  （goroutine + channel 并发生产）
StarRocks / ClickHouse → Go API       （双数据源 + 连接池 + context 超时 + 优雅关闭）
```

- 不改变主链路（Python 版仍是主体），Go 版独立部署
- 覆盖：goroutine 数量控制、channel 背压、连接池、context 取消、优雅退出

---

## 技术栈（最终，控制范围）

核心：**Python / SQL / Kafka / Flink / Spark / PySpark / StarRocks / ClickHouse / FastAPI / Docker / Cloud**

Go 为可选并行实现（V7 阶段引入）。

