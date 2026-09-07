# 金融交易实时数据平台 (Real-Time Financial Data Platform)

> 模拟金融交易场景的流批一体（Lambda）数据平台：数据生成 → 消息传输 → 实时/离线计算 → OLAP 存储 → 数据服务的完整链路。实时链路 Kafka → Flink → StarRocks，离线链路 MinIO → PySpark → ClickHouse，FastAPI 双数据源统一查询。

---

## 当前阶段：V3 —— 工程化 + Airflow 调度

V1（实时主链路 Kafka→Flink→StarRocks→API）、V2（Lambda 双链路：实时 Flink→StarRocks + 离线 PySpark→ClickHouse，Redis 维表打宽）已完成；当前在补工程化和 Airflow 定时调度。完整路线见 [docs/roadmap.md](docs/roadmap.md)。

```text
实时：  Generator → Kafka → Flink(清洗+打宽+窗口) → StarRocks ┐
                                                              ├→ FastAPI（双数据源）
离线：  MinIO → PySpark(ODS→DWD→DWS→ADS) → ClickHouse ───────┘
              维表 → Redis（T+1，供实时 Lookup 打宽）
```

---

## 技术分工

| 模块 | 语言 | 目录 | 职责 |
|---|---|---|---|
| 数据生成 | **Python** | [generator/](generator/) | 造模拟交易数据：Kafka Producer（实时）+ MinIO Parquet（历史） |
| 实时计算 | Flink SQL + Java UDF | [flink/](flink/) | 清洗、Event Time/Watermark、窗口聚合、Redis Lookup 打宽 |
| 实时 OLAP | SQL | [starrocks/](starrocks/) | 实时链路表建表 DDL（dwd_transaction_online / dws_realtime_agg 等） |
| 离线批处理 | Python / PySpark | [batch/](batch/) | T+1 数仓分层加工（ODS→DWD→DWS→ADS），详见 [batch/README.md](batch/README.md) |
| 离线 OLAP | SQL | [clickhouse/](clickhouse/) | 离线分层表建表 DDL（ODS/DWD/DWS/ADS 全层） |
| 查询服务 | **FastAPI (Python)** | [api/](api/) | REST API：StarRocks 查实时、ClickHouse 查离线 |
| 环境 | Docker | [deploy/](deploy/) | 一键启动 Kafka/Flink/StarRocks/ClickHouse/MinIO/Redis |

> Go 不在主链路。链路稳定后用 Go 重写 generator/API 作为并行实现（见 [roadmap 的 V7](docs/roadmap.md)）。

---

## 目录结构

```text
.
├── generator/    Python：数据生成（Kafka Producer + MinIO 历史数据）
├── flink/        实时计算作业（sql/ + udf/）
├── starrocks/    实时链路 OLAP 建表 DDL（dws_realtime_agg 等）
├── batch/        离线批链路：PySpark 数仓分层（ODS→DWD→DWS→ADS）
├── clickhouse/   离线链路 OLAP 建表 DDL（ods/dwd/dws/ads 全分层）
├── api/          FastAPI：查询服务（双数据源：StarRocks 实时 + ClickHouse 离线）
├── deploy/       docker-compose 环境
└── docs/         架构、路线图、分阶段 checklist、知识手册
```

---

## 启动步骤

两条链路独立运行、分开存储：**实时** Flink → StarRocks；**离线** PySpark → ClickHouse。

### 实时链路（Kafka → Flink → StarRocks → API）

```bash
# 1. 起环境
cd deploy && docker compose up -d

# 2. 建 StarRocks 实时表
#    执行 starrocks/ddl/ 下的建表语句（dws_realtime_agg、late_transaction）

# 3. 提交 Flink 作业
#    提交 flink/ 下的作业消费 Kafka 写入 StarRocks

# 4. 启动实时数据生成器（持续造数发 Kafka，Ctrl+C 退出）
cd generator && python send_realtime.py

# 5. 启动查询 API
cd api && uvicorn app.main:app --reload
```

### 离线批链路（MinIO → PySpark → ClickHouse，T+1）

```bash
# 1. 起 MinIO + ClickHouse（sync_dim_redis 还需要 Redis）
cd deploy && docker compose up -d minio clickhouse redis

# 2. 造历史数据 → MinIO（维表先行，交易引用维表 ID）
cd generator && python build_dimensions.py && python build_transactions.py

# 3. batch 环境（首次）+ ClickHouse 建分层表
cd ../batch && python3 -m venv .venv && source setup_env.sh
pip install -r requirements.txt
python -m src.io.clickhouse_admin ../clickhouse/ddl   # writer 写入前也会幂等自动建

# 4. 分层加工（ODS→DIM/DWD→DWS→ADS），依赖顺序执行，完整说明见 batch/README.md
#    维表贴源 + 维度表（无 dt，整表重刷）
python -m src.pipelines.ods_customer
python -m src.pipelines.ods_merchant
python -m src.pipelines.dim_customer
python -m src.pipelines.dim_merchant
#    事实链路（按业务日期 dt 分区，支持重跑）
python -m src.pipelines.ods_transaction 2026-09-04
python -m src.pipelines.dwd_transaction 2026-09-04
python -m src.pipelines.dws_customer_daily 2026-09-04
python -m src.pipelines.dws_merchant_daily 2026-09-04
python -m src.pipelines.ads_customer_profile 2026-09-04
python -m src.pipelines.ads_daily_report 2026-09-04
python -m src.pipelines.ads_merchant_top10 2026-09-04

# 5. ClickHouse 维表 → Redis（实时 Flink Lookup Join 打宽用）
python -m src.pipelines.sync_dim_redis
```

> 各模块的详细说明见各自目录下的 README.md（离线链路详见 [batch/README.md](batch/README.md)）。

---

## 参考文档

- [docs/architecture.md](docs/architecture.md) —— 架构与技术选型
- [docs/roadmap.md](docs/roadmap.md) —— V1~V7 分阶段路线
