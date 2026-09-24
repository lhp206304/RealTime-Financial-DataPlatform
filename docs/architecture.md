# 架构与技术选型

> 提炼自项目原始设计文档。

## 定位

模拟金融交易场景的流批一体数据平台，覆盖的能力链：

**传统数仓 → 实时数仓 → 流批一体 → OLAP → 工程化 → Cloud**。

---

## 完整架构（最终形态）

```text
                    调度层：Airflow（每日 T+1）
        generator 每日演进（增/改/软删维度 + 造当天流水）
            │ 触发并编排
            ▼
                         Data Sources
              generator（实时流）   generator（每日批量）
                   │                      │
                   ↓                      ↓
                 Kafka               MinIO (Data Lake)
                   ↓                     ↓
                 Flink                PySpark
                   │ Lookup 打宽          ↓
                   │              dim_*（维度表）
                   │                     │ T+1 同步
                   └──────▶ Redis ◀───────┘
                   ↓                     ↓
              StarRocks (OLAP)     ClickHouse (OLAP)
                   └──────────┬──────────┘
                              ↓
                    FastAPI（双数据源路由）
                              │
                 ┌────────────┼────────────┐
                 ↓            ↓            ↓
                BI          API           ML
```

架构形态（Lambda）：**实时链路（速度层）和离线链路（批处理层）独立计算、分开存储**——Flink 实时结果写 StarRocks，PySpark T+1 分层结果写 ClickHouse；维表由离线侧产出、T+1 同步 Redis 供 Flink Lookup 打宽；FastAPI 按数据来源路由到对应引擎。离线链路（每日造数 + 数仓分层）由 **Airflow 统一调度**，任务定义在 `airflow/dags/config/warehouse_tables.yml`，一个 DAG 串起全链路。

---

## 技术分梯队

| 梯队 | 技术 | 作用 |
|---|---|---|
| 第一（核心） | Kafka + Flink + StarRocks + FastAPI | 实时数仓主线 + 数据服务 |
| 第二（数据工程） | Python + PySpark + ClickHouse + MinIO + Redis | 离线数仓分层、维表打宽、特征工程 |
| 第三（工程化） | Docker + Airflow | 离线任务调度、容器化（已落地） |
| 第四（待建设） | GitHub Actions + Cloud | CI/CD、云实践（V6） |

---

## 语言分工

主链路用 Python / SQL，实时维表打宽的 UDF 用 Java：

| 模块 | 技术 | 说明 |
|---|---|---|
| 数据生成 + Producer | **Python** (Pydantic + confluent-kafka) | 复用 Pydantic 校验 |
| 查询 API | **FastAPI (Python)** | 自带 Pydantic 校验 + OpenAPI 文档 |
| 实时计算 Flink | Flink SQL（+ Java UDF） | 作业主体是 SQL；Redis 维表打宽用 Java 写的 `redis_hash` UDF |
| 离线 PySpark | Python | 一致 |
| 调度 Airflow | Python（YAML 驱动动态 DAG） | DAG 读 `warehouse_tables.yml` 生成任务，加表只改配置 |

### Go 作为可选并行实现（见 roadmap 的 V4）

Go 不进入主链路。链路稳定后，用 Go **重写查询 API** 作为并行实现（已完成）：

- 不改变主链路（Python 版仍是主体），Go 版独立部署（端口 8001）
- 覆盖 Go 高并发场景：goroutine 数量控制、连接池、context 超时、优雅关闭

---
