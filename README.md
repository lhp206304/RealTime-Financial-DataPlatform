# 金融交易实时数据平台 (Real-Time Financial Data Platform)

模拟金融交易场景的实时数据平台，采用 **Lambda 架构**（速度层 + 批处理层），支撑三类业务场景：

- **实时交易监控**：Flink 清洗、去重、维表打宽后的交易明细，秒级可查
- **实时窗口聚合**：TUMBLE / HOP 窗口产出客户维度分钟级指标
- **T+1 离线分析**：PySpark 数仓分层（ODS→DIM→DWD→DWS→ADS），客户画像与经营大盘
- **统一查询服务**：FastAPI 按数据来源路由双 OLAP 引擎，一套 API 同时取实时与离线数据

覆盖完整链路：数据生成 → 消息传输 → 流/批计算 → 双 OLAP 存储 → 数据服务。

---

## 架构

```text
离线（批处理层 T+1）
  MinIO Parquet ──► PySpark（ODS→DIM→DWD→DWS→ADS）──► ClickHouse
                                                        │ dim_* T+1 同步
                                                        ▼
                                                     Redis（维表缓存）
                                                        ▲ Lookup 打宽
实时（速度层）                                            │
  Kafka(transaction) ──► Flink Job1（清洗+去重+查Redis打宽+迟到侧输出）
                             │ 双写：Kafka(dwd_transaction) + StarRocks 明细
                             ▼
                        Kafka(dwd_transaction) ──► Flink Job2（Watermark+Checkpoint+TUMBLE/HOP 窗口）
                                                        │
                                                        ▼
                                                   StarRocks
                                                        │
服务层：FastAPI（按表来源路由：实时表→StarRocks / 离线表→ClickHouse）
```

### 选型理由

| 决策 | 理由 |
|---|---|
| Lambda 双引擎（StarRocks + ClickHouse） | StarRocks 主键模型擅长实时 upsert / 点查，承接 Flink 持续写入；ClickHouse MergeTree 擅长大批量扫描，承接 PySpark T+1 全量分层。离线大批量写入不与实时查询争抢资源。代价是 API 维护双数据源，由 `db.py` 统一封装两种方言 |
| 维表放 Redis 而非直连 OLAP | Flink Lookup 是高频点查，直连 OLAP 会产生大量随机读；维度数据 T+1 才更新，天然适合缓存。打宽用自研 Java UDTF（`RedisHashLookupFunction`）实现 |
| Kafka topic 分层（transaction → dwd_transaction） | 层间用 topic 解耦：Job1 产 DWD 宽表流，Job2 只订阅 DWD topic 做窗口聚合，两层独立扩展、互不影响 |
| StarRocks 走 Connector 而非 Routine Load | 清洗、打宽、聚合逻辑放在 Flink 流处理层，StarRocks 只接收成品结果，避免 OLAP 层重复实现计算口径 |

---

## 技术栈

| 层 | 技术 | 版本 |
|---|---|---|
| 数据生成 | Python + Pydantic + confluent-kafka | 3.14 |
| 消息队列 | Kafka（KRaft 模式，免 ZooKeeper，transaction topic 3 分区） | 4.3.1 |
| 实时计算 | Flink SQL + Java UDF（Redis Lookup） | 1.20 |
| 离线计算 | PySpark（local 模式，Spark Connector 写 ClickHouse） | 4.2 |
| 实时 OLAP | StarRocks（Primary Key 模型，upsert 幂等） | 3.5 |
| 离线 OLAP | ClickHouse（MergeTree / ReplacingMergeTree，toYYYYMM 月分区） | 24 |
| 维表缓存 | Redis（Hash 存维度属性） | 7 |
| 对象存储 | MinIO（S3 兼容，存 Parquet：fact / dim 两桶） | latest |
| 查询服务 | FastAPI + Pydantic + SQLAlchemy + clickhouse-connect | — |
| 部署 | 基础设施 8 服务 Docker Compose 一键启动；应用层（generator / batch / api）本地 venv 运行 | — |

---

## 快速复现

> 部署形态：**基础设施容器化、应用层本地运行**。Kafka / StarRocks / Flink / MinIO / Redis / ClickHouse 跑在 Docker；数据生成、离线跑批、查询服务在宿主机各自的 Python venv 里跑。

### ① 本地依赖（一次性准备）

| 依赖 | 用途 | 安装 |
|---|---|---|
| Docker + Compose | 基础设施 8 服务 | Docker Desktop |
| JDK 17 | PySpark JVM / Flink UDF 编译 | `brew install openjdk@17` |
| Maven | UDF 打包 | `brew install maven` |
| Python 3.14 | generator / batch / api 三个独立 venv | python.org 或 pyenv |


### ② 起基础设施

```bash
docker compose -f deploy/docker-compose.yml up -d
```

验证：`docker ps` 看到 broker / starrocks / jobmanager / taskmanager / minio / redis / clickhouse 全部 Up；`init-kafka` 一次性容器自动建好 3 分区 topic `transaction`。

### ③ 三个应用模块的环境（各一个 venv）

```bash
# generator：造数（Pydantic + confluent-kafka）
cd generator
python3.14 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# batch：离线跑批（PySpark 4.2）
cd ../batch
python3.14 -m venv .venv
source setup_env.sh    # 一键激活 venv + JAVA_HOME + PYTHONPATH + PYSPARK_PYTHON（需先装 JDK 17）

# api：查询服务（FastAPI）
cd ../api
python3.14 -m venv .venv && source .venv/bin/activate
pip install fastapi "uvicorn[standard]" sqlalchemy pymysql clickhouse-connect
```

### ④ 造数（generator venv，宿主机）

```bash
cd generator && source .venv/bin/activate
python build_dimensions.py      # 客户/账户/商户维表 → MinIO dim 桶
python build_transactions.py    # 批量历史交易 → MinIO fact 桶（默认 10000 条）
python send_realtime.py         # 实时交易流 → Kafka（持续发送，Ctrl+C 退出）
```

验证：MinIO 控制台（`localhost:9001`）能看到 `fact/fact_transaction.parquet`、`dim/*.parquet`。

### ⑤ 建表

```bash
# ClickHouse 11 张（ODS/DIM/DWD/DWS/ADS，走 HTTP 8123；batch venv）
cd ../batch && source setup_env.sh
python -m src.io.clickhouse_admin ../clickhouse/ddl

# StarRocks 2 张（FE MySQL 协议 9030；宿主机 mysql 客户端）
mysql -h 127.0.0.1 -P 9030 -u root < ../starrocks/ddl/dws_realtime_agg.sql
mysql -h 127.0.0.1 -P 9030 -u root < ../starrocks/ddl/late_transaction.sql
```

> 实时明细表 `dwd_transaction_online` 的表结构定义在 Flink Sink 连接器里（[flink/sql/kafka_to_starrocks.sql](flink/sql/kafka_to_starrocks.sql)）。
> batch 的 writer 写入前会按 DDL 幂等建表（`ensure_table`），ClickHouse 部分手动预建可省略。

### ⑥ 离线跑批（batch venv，按依赖序）

```bash
DT=$(date +%F)
# 维表组（T+1 全量，无业务日期）
python -m src.pipelines.ods_customer && python -m src.pipelines.dim_customer
python -m src.pipelines.ods_merchant && python -m src.pipelines.dim_merchant
python -m src.pipelines.sync_dim_redis        # 维表 → Redis Hash

# 事实链路（按业务日期逐层推进）
python -m src.pipelines.ods_transaction $DT
python -m src.pipelines.dwd_transaction $DT    # 清洗 + JOIN 维表打宽
python -m src.pipelines.dws_customer_daily $DT
python -m src.pipelines.dws_merchant_daily $DT
python -m src.pipelines.ads_customer_profile $DT
python -m src.pipelines.ads_daily_report $DT
python -m src.pipelines.ads_merchant_top10 $DT
```

验证：每个任务输出质量校验结果（行数对比 / NULL 率）；同一天重跑不产生重复数据（按 dt 分区覆盖写）。

### ⑦ 构建 UDF 并重启 Flink（首次）

```bash
cd ../flink/udf && mvn package
cp target/flink-udf-1.0.jar ../lib/
docker compose -f ../../deploy/docker-compose.yml up -d jobmanager taskmanager
```

### ⑧ 提交 Flink 作业（容器内 SQL Client）

```bash
docker exec -i jobmanager ./bin/sql-client.sh -f /opt/flink/sql/kafka_to_starrocks.sql   # Job1：清洗+打宽
docker exec -i jobmanager ./bin/sql-client.sh -f /opt/flink/sql/job2.sql                 # Job2：窗口聚合
```

验证：Web UI（`localhost:8081`）两个作业 RUNNING，Checkpoint 周期性成功；`send_realtime.py` 持续发数后，StarRocks `dwd_transaction_online` / `dws_realtime_agg` 有数据且打宽字段非 NULL。

### ⑨ 启动查询服务（api venv，宿主机）

```bash
cd ../api && source .venv/bin/activate
uvicorn app.main:app --port 8000
```

```bash
curl http://localhost:8000/health
curl http://localhost:8000/transactions/realtime
curl http://localhost:8000/customers/C12050/full-profile   # 实时统计 + 离线画像一次返回
```

接口文档（OpenAPI 自动生成）：`http://localhost:8000/docs`

---

## 模块说明

| 模块 | 职责 | 文档 |
|---|---|---|
| [generator/](generator/) | 模拟数据生成：维表 + 历史交易落 MinIO，实时流发 Kafka | [generator/README.md](generator/README.md) |
| [flink/](flink/) | 实时链路：Job1 清洗打宽双写、Job2 窗口聚合；Java UDF 源码 | [flink/README.md](flink/README.md) |
| [batch/](batch/) | 离线链路：PySpark 数仓分层 + 维表同步 Redis | [batch/README.md](batch/README.md) |
| [clickhouse/](clickhouse/) | 离线 OLAP 建表 DDL（11 张） | [clickhouse/ddl/](clickhouse/ddl/) |
| [starrocks/](starrocks/) | 实时 OLAP 建表 DDL（聚合表 + 迟到明细表） | [starrocks/README.md](starrocks/README.md) |
| [api/](api/) | 查询服务：双数据源路由（StarRocks / ClickHouse） | [api/README.md](api/README.md) |
| [deploy/](deploy/) | Docker Compose 一键环境 | [deploy/README.md](deploy/README.md) |
| [docs/](docs/) | 架构说明、分阶段 checklist、故障排查 | [docs/architecture.md](docs/architecture.md) |

---

## 技术设计要点

### 流批架构

- **Lambda 而非 Kappa**：金融场景要求「实时看当下、离线保准确」，两条链路各算一遍后在服务层合并。Kappa 靠回放重算历史，大批量回放成本高、OLAP 能力弱。
- **层间协议统一**：实时（Flink）与离线（PySpark）的 schema 都对齐 [generator/schema.py](generator/schema.py) 的 Pydantic 模型，服务层才能对同一 `customer_id` 无缝合并两类结果。

### 数据正确性

- **事件时间语义**：Flink 用 `WATERMARK FOR event_time - INTERVAL '5' SECOND` 容忍乱序；超过 watermark 的事件不丢弃，单独侧输出到迟到明细表，可回溯补数。
- **去重**：Kafka 生产端 `acks=all + retries`，下游按 `transaction_id` 开窗取最新（`row_number`）；StarRocks 主键模型按主键 upsert，三层兜底保证端到端不重复。
- **幂等重跑**：离线事实表按 `dt` 分区先删后写，维表整表覆盖，同一天任务重跑/补数不产生重复数据；ClickHouse 明细表用 MergeTree + 写入前 DELETE PARTITION 达成同样语义。
- **金融精度**：金额全程 `Decimal`（Kafka JSON 里是字符串、Parquet/OLAP 里是 DECIMAL(18,2)），规避浮点误差；`event_time` 统一 UTC 带时区、截断到毫秒。

### 维表与打宽

- **主数据先行**：维表 ID 池由 generator 先生成，交易只引用已有 ID，从源头杜绝 JOIN 不上的孤儿 ID。
- **打宽链路**：维表源头在 ClickHouse（PySpark 加工），T+1 批量同步进 Redis（Hash 结构，key = `dim:customer:{id}`）；Flink 通过自研 Java UDTF `RedisHashLookupFunction` 逐条 Lookup；Redis 未命中给默认值，不让主流卡住。

### 工程化

- **一表一模块**：离线 12 个 pipeline 模块，「一个文件 = 一张目标表 = 一个可调度任务」，`run(dt)` 接收业务日期参数，可直接映射为 Airflow DAG task，作业代码零改动。
- **纯函数可单测**：pipeline 内业务函数（clean / enrich / aggregate）只做 DataFrame→DataFrame，不碰 IO，pytest 造假 DataFrame 即可覆盖；IO 集中在 `src/io/` 读写层。
- **配置分离**：Pydantic Settings 管理连接参数，`BATCH_*` 环境变量覆盖默认值，本机 / 容器网络切换不改代码。
- **质量前置**：每层加工后在写入前做质量校验（行数对比 / NULL 率 / 聚合比），脏数据拦截在进入下游之前。
- **API 工程化**：SQLAlchemy 连接池（pool_recycle + pre_ping 防断连）、双链路查询统一 5 秒超时、全量命名参数绑定防注入、Pydantic 响应模型校验。

---

## 路线图

| 阶段 | 内容 | 状态 |
|---|---|---|
| V1 | 实时主链路（Generator→Kafka→Flink→StarRocks→FastAPI） | ✅ 完成 |
| V2 | 离线分层 + 维表打宽 + Flink 进阶（Watermark / Checkpoint / 窗口），演进为 Lambda 双引擎 | ✅ 完成 |
| V3 | 工程化 + Airflow 调度（12 个 pipeline 已按 task 组织，DAG 待落地） | 🔨 进行中 |
| V4 | 数据质量模块（独立 DQ 规则 + 质量报告） | 规划中 |
| V5 | 风险模型（特征工程 → 训练 → Flink 实时评分） | 规划中 |
| V6 | Cloud（对象存储迁移 + CI/CD） | 规划中 |
| V7 | Go 并行实现 generator / API（高并发） | 规划中 |
