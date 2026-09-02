# 架构与技术选型

> 提炼自项目原始设计文档。

## 定位

模拟金融交易场景的流批一体数据平台。核心竞争力不是「会很多技术」，而是：

**传统数仓 → 实时数仓 → 流批一体 → OLAP → 工程化 → Cloud** 的完整能力链。

---

## 完整架构（最终形态）

```text
                         Data Sources
                              │
                   ┌──────────┴──────────┐
                   ↓                     ↓
                Real-time             Historical
                   ↓                     ↓
                 Kafka                  R2 (Data Lake)
                   ↓                     ↓
                 Flink                PySpark
                   └──────────┬──────────┘
                              ↓
                         StarRocks (OLAP)
                              │
                 ┌────────────┼────────────┐
                 ↓            ↓            ↓
                BI          API           ML
```

流批一体含义：**实时链路和离线链路独立计算，最终汇聚到统一 OLAP 层 StarRocks。**

---

## 技术分梯队

| 梯队 | 技术 | 作用 |
|---|---|---|
| 第一（核心） | Kafka + Flink + StarRocks | 实时数仓主线 |
| 第二（数据工程） | Python + PySpark + SQL | 离线计算、特征工程 |
| 第三（工程化） | Docker + GitHub Actions + Cloud | 工程化 & 云实践 |

---

## 语言分工

目标岗位是 **Data Engineer**，所以主链路全部用 Python / SQL，与主流 Data Engineer 技术栈对齐：

| 模块 | 技术 | 说明 |
|---|---|---|
| 数据生成 + Producer | **Python** (Pydantic + confluent-kafka) | 复用 Pydantic 校验 |
| 查询 API | **FastAPI (Python)** | Data Engineer 岗事实标准，自带 Pydantic 校验 + OpenAPI 文档 |
| 实时计算 Flink | Python / SQL | 一致 |
| 离线 PySpark | Python | 一致 |

### Go 作为独立练习阶段（见 roadmap 的 V7）

Go 不进入主链路，而是在链路跑通后，用 Go **重写 generator 和 API** 作为高并发练习：

- 简历上形成 **Python(主) + Go(加分)** 双标签
- 集中练 Go 高并发：goroutine、channel、连接池、context 超时、优雅关闭
- 不影响主链路，纯加分项

---

## Cloud 选型

待定
