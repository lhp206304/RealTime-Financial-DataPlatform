# 架构与技术选型

> 提炼自项目原始设计文档。

## 定位

模拟金融交易场景的流批一体数据平台，覆盖的能力链：

**传统数仓 → 实时数仓 → 流批一体 → OLAP → 工程化 → Cloud**。

---

## 完整架构（最终形态）

```text
                         Data Sources
                              │
                   ┌──────────┴──────────┐
                   ↓                     ↓
                Real-time             Historical
                   ↓                     ↓
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

架构形态（Lambda）：**实时链路（速度层）和离线链路（批处理层）独立计算、分开存储**——Flink 实时结果写 StarRocks，PySpark T+1 分层结果写 ClickHouse；维表由离线侧产出、T+1 同步 Redis 供 Flink Lookup 打宽；FastAPI 按数据来源路由到对应引擎。

---

## 技术分梯队

| 梯队 | 技术 | 作用 |
|---|---|---|
| 第一（核心） | Kafka + Flink + StarRocks + FastAPI | 实时数仓主线 + 数据服务 |
| 第二（数据工程） | Python + PySpark + ClickHouse + MinIO + Redis | 离线数仓分层、维表打宽、特征工程 |
| 第三（工程化） | Docker + Airflow + GitHub Actions + Cloud | 离线任务调度、工程化 & 云实践 |

---

## 语言分工

主链路用 Python / SQL，实时维表打宽的 UDF 用 Java：

| 模块 | 技术 | 说明 |
|---|---|---|
| 数据生成 + Producer | **Python** (Pydantic + confluent-kafka) | 复用 Pydantic 校验 |
| 查询 API | **FastAPI (Python)** | 自带 Pydantic 校验 + OpenAPI 文档 |
| 实时计算 Flink | Flink SQL（+ Java UDF） | 作业主体是 SQL；Redis 维表打宽用 Java 写的 `redis_hash` UDF |
| 离线 PySpark | Python | 一致 |

### Go 作为可选并行实现（见 roadmap 的 V7）

Go 不进入主链路。链路稳定后，可用 Go **重写 generator 和 API** 作为并行实现：

- 不改变主链路（Python 版仍是主体），Go 版独立部署验证
- 覆盖 Go 高并发场景：goroutine 数量控制、channel 背压、连接池、context 超时、优雅关闭

---

## Cloud 选型

待定
