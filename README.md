# 金融交易实时数据平台 (Real-Time Financial Data Platform)

模拟金融交易场景的**实时数据平台**，覆盖：数据生成 → 消息传输 → 实时计算 → OLAP 存储 → 数据服务。

> 定位：简历强化项目。核心目标是证明「能设计并实现一个现代实时数据平台，理解 Kafka / Flink / OLAP / 数据建模 / 工程化如何协作」。

---

## 当前阶段：V1 —— 实时主链路

```text
Python Data Generator  →  Kafka  →  Flink  →  StarRocks  →  FastAPI
    (造数+Producer)      (transaction)  (清洗+窗口聚合)  (实时数仓/OLAP)  (查询服务)
```

V1 的唯一目标：**把这条端到端链路跑通**。其他阶段（离线 PySpark、数据质量、ML、Cloud）见 [docs/roadmap.md](docs/roadmap.md)。

---

## 技术分工

| 模块 | 语言 | 目录 | 职责 |
|---|---|---|---|
| 数据生成 | **Python** | [generator/](generator/) | 造模拟交易数据 + Kafka Producer |
| 实时计算 | Python / SQL | [flink/](flink/) | 清洗、Event Time/Watermark、窗口聚合 |
| OLAP 存储 | SQL | [starrocks/](starrocks/) | ODS/DWD/DWS/ADS 分层建表 |
| 查询服务 | **FastAPI (Python)** | [api/](api/) | REST API 查实时指标 |
| 环境 | Docker | [deploy/](deploy/) | 一键启动 Kafka/Flink/StarRocks |

> Go 不在 V1 主链路。链路跑通后用 Go 重写 generator/API 作为高并发练习（见 [roadmap 的 V7](docs/roadmap.md)）。

---

## 目录结构

```text
.
├── generator/    Python：数据生成 + Kafka Producer
├── flink/        实时计算作业（sql/ + jobs/）
├── starrocks/    OLAP 建表 DDL + 数据导入
├── api/          FastAPI：查询服务
├── deploy/       docker-compose 环境
└── docs/         架构、路线图、分阶段 checklist、知识手册
```

---

## 启动步骤（V1 目标状态）

```bash
# 1. 起环境
cd deploy && docker compose up -d

# 2. 建 StarRocks 表
#    执行 starrocks/ddl/ 下的建表语句

# 3. 提交 Flink 作业
#    提交 flink/ 下的作业消费 Kafka 写入 StarRocks

# 4. 启动数据生成器
cd generator && python -m generator

# 5. 启动查询 API
cd api && uvicorn app.main:app --reload
```

> 每个模块的具体实现清单见各自目录下的 README.md。核心代码由本人编写。

---

## 参考文档

- [docs/architecture.md](docs/architecture.md) —— 架构与技术选型
- [docs/roadmap.md](docs/roadmap.md) —— V1~V6 分阶段路线
