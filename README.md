# 金融交易实时数据平台 (Real-Time Financial Data Platform)

模拟金融交易场景的实时数据平台，采用 **Lambda 架构**（速度层 + 批处理层），支撑三类业务场景：

- **实时交易监控**：Flink 清洗、去重、维表打宽后的交易明细，秒级可查
- **实时窗口聚合**：TUMBLE / HOP 窗口产出客户维度分钟级指标
- **T+1 离线分析**：PySpark 数仓分层（ODS→DIM→DWD→DWS→ADS），客户画像与经营大盘
- **统一查询服务**：FastAPI 按数据来源路由双 OLAP 引擎，一套 API 同时取实时与离线数据

覆盖完整链路：数据生成 → 消息传输 → 流/批计算 → 双 OLAP 存储 → 数据服务。离线批链路由 **Airflow** 统一调度（每日自动造数 + 分层跑批）。

---

## 架构

```text
离线（批处理层 T+1）—— 由 Airflow 每日自动调度
  generator 每日造数（演进维度 + 造当天流水）
        │
        ▼
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
服务层：FastAPI + go-api（按表来源路由：实时表→StarRocks / 离线表→ClickHouse）
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
| 离线计算 | PySpark（Spark Standalone 集群，Airflow SparkSubmitOperator 提交；手动调试可 local 模式） | 4.2 |
| 实时 OLAP | StarRocks（Primary Key 模型，upsert 幂等） | 3.5 |
| 离线 OLAP | ClickHouse（MergeTree / ReplacingMergeTree，toYYYYMM 月分区） | 24 |
| 维表缓存 | Redis（Hash 存维度属性） | 7 |
| 对象存储 | MinIO（S3 兼容，数仓上游数据湖：master 主数据桶 / transaction 流水桶） | latest |
| 查询服务 | FastAPI + Pydantic + SQLAlchemy + clickhouse-connect | — |
| 查询服务（Go） | Go 1.27 + Chi + sqlx + clickhouse-go/v2 | 1.27 |
| 调度 | Airflow（LocalExecutor，YAML 驱动的动态 DAG，每日造数 + 分层跑批） | 2.9.3 |
| 部署 | 全部服务 Docker Compose 一键启动（基础设施 + Spark + Airflow + api）；仅 Flink UDF 打包与实时流演示脚本在本地执行 | — |

---

## 快速复现

> 部署形态：**全量 Docker Compose 容器化**。Kafka / StarRocks / Flink / MinIO / Redis / ClickHouse / Spark / Airflow / api 全部跑在容器里；跑批与每日造数由 Airflow 在容器内执行，ClickHouse 表首次写入时自动幂等创建。
> 本地只需做两件事：**首次打 Flink UDF jar**（需 JDK 17 + Maven）、**跑实时流演示** `send_realtime.py`（可选，喂 Flink 实时链路，需本地 Python venv）。

### ① 本地依赖（一次性准备）

| 依赖 | 用途 | 安装 |
|---|---|---|
| Docker + Compose | 全部 15 个服务（含 Spark / Airflow / api） | Docker Desktop |
| DBeaver（可选） | 数据库 GUI，连 ClickHouse / StarRocks 看数据 | `brew install --cask dbeaver-community` |
| JDK 17 + Maven | **仅首次**打 Flink UDF jar 用 | `brew install openjdk@17 maven` |
| Python 3.14 | **仅跑实时流演示** `send_realtime.py` 时需要（可选） | python.org 或 pyenv |

> **中间件本地零安装**：ClickHouse、StarRocks、MinIO、Kafka、Redis、Flink、Spark、Airflow、Postgres 全部由 Docker Compose 引用公共镜像，`up -d --build` 时本地没有就自动从 Docker Hub 拉取，无需在 macOS 上安装任何数据库或 MinIO 本体。MinIO 控制台直接用浏览器访问 `localhost:9001`（minioadmin / minioadmin），也不用装客户端。

**DBeaver 连接参数**（连的是 Docker 映射到宿主机的端口）：

| 数据库 | DBeaver 选的驱动 | Host:Port | 账号 | 库 |
|---|---|---|---|---|
| ClickHouse | ClickHouse（DBeaver 24+ 官方驱动，走 HTTP） | `localhost:8123` | default / 空密码 | finance |
| StarRocks | MySQL（StarRocks 兼容 MySQL 协议） | `localhost:9030` | root / 空密码 | finance |

### ② 起全部服务

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

验证：`docker ps` 看到 broker / starrocks / jobmanager / taskmanager / minio / redis / clickhouse / spark-master / spark-worker / airflow-webserver / airflow-scheduler / api / go-api 全部 Up；`init-kafka` 一次性容器自动建好 3 分区 topic `transaction`。

> - Airflow Web UI：`http://localhost:8088`（admin / admin）。airflow-scheduler 会等 minio / redis / clickhouse / spark-master 的 healthcheck 通过后才启动。
> - Python 查询服务 api：`http://localhost:8000/docs`（也可单独 `docker compose -f deploy/docker-compose.yml up -d api`）。
> - Go 查询服务 go-api：`http://localhost:8001/health`（也可单独 `docker compose -f deploy/docker-compose.yml up -d go-api`）。
> - MinIO 控制台：`http://localhost:9001`（minioadmin / minioadmin），在容器里运行，浏览器直接访问，无需本地安装。

### ③ 首次铺底数据（容器内执行，无需本地 venv）

全新环境先铺一次维度主数据和历史交易（之后由 Airflow 每日自动演进/造数）：

```bash
# 在 airflow-scheduler 容器里跑（generator 已挂载到 /opt/generator，boto3 已装，MINIO_* 已注入）
docker exec -w /opt/generator airflow-scheduler python3 build_dimensions.py     # 主数据 → master 桶
docker exec -w /opt/generator airflow-scheduler python3 build_transactions.py   # 历史交易 → transaction 桶
```

验证：MinIO 控制台（`localhost:9001`）能看到 `transaction/fact_transaction.parquet`、`master/*.parquet`。

### ④ 建 StarRocks 表（仅实时链路 2 张，ClickHouse 表无需手动建）

ClickHouse 的 11 张表由 batch writer 在首次写入前调 `ensure_table` 按 DDL **自动幂等创建**，无需手动执行。StarRocks 的物理表 Flink connector 不会自动建，需手动执行一次：

```bash
# 宿主机有 mysql 客户端
mysql -h 127.0.0.1 -P 9030 -u root < starrocks/ddl/dws_realtime_agg.sql
mysql -h 127.0.0.1 -P 9030 -u root < starrocks/ddl/late_transaction.sql

# 或在 starrocks 容器内执行（无需宿主机装 mysql）
docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -u root < starrocks/ddl/dws_realtime_agg.sql
docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -u root < starrocks/ddl/late_transaction.sql
```

> 实时明细表 `dwd_transaction_online` 的表结构定义在 Flink Sink 连接器里（[flink/sql/kafka_to_starrocks.sql](flink/sql/kafka_to_starrocks.sql)）。

### ⑤ 离线跑批

**推荐方式：Airflow 自动调度（每日全链路）**

在 Airflow Web UI（`localhost:8088`）触发 `warehouse_dynamic` DAG，或命令行：

```bash
# 手动触发当天的全链路（造数 → ODS → DIM → DWD → DWS → ADS → Redis）
docker exec airflow-scheduler airflow dags trigger warehouse_dynamic
```

DAG 链路：`evolve_dimensions → generate_transactions → ods → dim → dwd → dws → ads → sync_dim_redis`，每日自动执行，任务间依赖由 DAG 声明。

**手动方式（调试单表，全部在 spark-master 容器内执行）：**

```bash
DT=$(date +%F)
COMPOSE="docker compose -f ./deploy/docker-compose.yml exec -T spark-master"
SUBMIT="/opt/spark/bin/spark-submit --master local[*]"

# 维表组（T+1 全量，无业务日期；除 sync_dim_redis 外都走 spark-submit）
$COMPOSE $SUBMIT src/pipelines/ods_customer.py
$COMPOSE $SUBMIT src/pipelines/dim_customer.py
$COMPOSE $SUBMIT src/pipelines/ods_merchant.py
$COMPOSE $SUBMIT src/pipelines/dim_merchant.py
$COMPOSE python3 -m src.pipelines.sync_dim_redis        # 纯 Python：维表 → Redis Hash

# 事实链路（按业务日期逐层推进）
$COMPOSE $SUBMIT src/pipelines/ods_transaction.py $DT
$COMPOSE $SUBMIT src/pipelines/dwd_transaction.py $DT    # 清洗 + JOIN 维表打宽
$COMPOSE $SUBMIT src/pipelines/dws_customer_daily.py $DT
$COMPOSE $SUBMIT src/pipelines/dws_merchant_daily.py $DT
$COMPOSE $SUBMIT src/pipelines/ads_customer_profile.py $DT
$COMPOSE $SUBMIT src/pipelines/ads_daily_report.py $DT
$COMPOSE $SUBMIT src/pipelines/ads_merchant_top10.py $DT
```

验证：每个任务输出质量校验结果（行数对比 / NULL 率）；同一天重跑不产生重复数据（按 dt 分区覆盖写）。

### ⑥ 构建 UDF 并重启 Flink（首次）

```bash
cd flink/udf && mvn package
cp target/flink-udf-1.0.jar ../lib/
docker compose -f deploy/docker-compose.yml up -d jobmanager taskmanager
```

### ⑦ 提交 Flink 作业（容器内 SQL Client）

```bash
docker exec -i jobmanager ./bin/sql-client.sh -f /opt/flink/sql/kafka_to_starrocks.sql   # Job1：清洗+打宽
docker exec -i jobmanager ./bin/sql-client.sh -f /opt/flink/sql/job2.sql                 # Job2：窗口聚合
```

验证：Web UI（`localhost:8081`）两个作业 RUNNING，Checkpoint 周期性成功。

### ⑧ 实时流演示（唯一需要本地 venv 的脚本，可选）

Flink 作业需要实时数据喂入。`send_realtime.py` 连 `localhost:9092`（且 confluent-kafka 未装进容器镜像），在宿主机 venv 运行：

```bash
cd generator
python3.14 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python send_realtime.py    # 每 0.5~2 秒造一条发 Kafka，Ctrl+C 优雅退出
```

跑起来后 StarRocks `dwd_transaction_online` / `dws_realtime_agg` 开始有数据且打宽字段非 NULL。

### ⑨ 验证查询服务（两个 API 随 compose 启动）

Python 版 api 容器在步骤②已启动（`finance-api:latest`，端口 8000）：

```bash
curl http://localhost:8000/health
curl http://localhost:8000/transactions/realtime
curl http://localhost:8000/customers/C12050/full-profile   # 实时统计 + 离线画像一次返回
```

接口文档（OpenAPI 自动生成）：`http://localhost:8000/docs`。单独重启：`docker compose -f deploy/docker-compose.yml restart api`。

Go 版 go-api 容器在步骤②已启动（`go-api:latest`，端口 8001），接口契约与 Python 版一致：

```bash
curl http://localhost:8001/health
curl "http://localhost:8001/transactions?customer_id=1&limit=3"
curl http://localhost:8001/customers/1/full-profile
```

单独重启：`docker compose -f deploy/docker-compose.yml restart go-api`。

---

## 模块说明

| 模块 | 职责 | 文档 |
|---|---|---|
| [generator/](generator/) | 模拟数据生成：主数据 + 历史交易落 MinIO，实时流发 Kafka；每日维度演进（增/改/软删）| [generator/README.md](generator/README.md) |
| [airflow/](airflow/) | 调度编排：YAML 驱动的动态 DAG，每日造数 + 分层跑批 | — |
| [flink/](flink/) | 实时链路：Job1 清洗打宽双写、Job2 窗口聚合；Java UDF 源码 | [flink/README.md](flink/README.md) |
| [batch/](batch/) | 离线链路：PySpark 数仓分层 + 维表同步 Redis | [batch/README.md](batch/README.md) |
| [shared/](shared/) | 共享模块：structlog + 标准 logging 桥接配置，供 api/batch/airflow 复用 | — |
| [clickhouse/](clickhouse/) | 离线 OLAP 建表 DDL（11 张） | [clickhouse/ddl/](clickhouse/ddl/) |
| [starrocks/](starrocks/) | 实时 OLAP 建表 DDL（聚合表 + 迟到明细表） | [starrocks/README.md](starrocks/README.md) |
| [api/](api/) | 查询服务（Python）：双数据源路由（StarRocks / ClickHouse） | [api/README.md](api/README.md) |
| [go-api/](go-api/) | 查询服务（Go）：与 api 接口契约一致，Chi + sqlx + distroless 容器化 | [go-api/README.md](go-api/README.md) |
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
| V3 | 工程化 + Airflow 调度（YAML 动态 DAG + 每日造数 + 分层跑批，已接入 generator 每日演进） | ✅ 完成 |
| V4 | Go 重写查询 API（高并发，接口契约与 Python 版一致） | ✅ 完成 |
