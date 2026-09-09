# batch —— 离线批链路（PySpark 数仓分层加工）

> V3 流批一体的离线部分：MinIO 历史数据 → PySpark 走完整数仓分层（ODS→DWD→DWS→ADS）→ ClickHouse。
> 与实时链路**独立算、分开存**：离线 T+1 写 ClickHouse，实时 Flink 写 StarRocks，API 层双数据源查询。

***

## 整体架构

```text
          ┌───────────────────────────────────────────────────┐
          │          ClickHouse（离线 OLAP 汇聚层）             │
          │  dim_customer_offline  dim_merchant_offline       │
          │  ods_transaction → dwd_transaction_offline        │
          │       ↓                  ↓                          │
          │  dws_customer_daily  dws_merchant_daily            │
          │       ↓                  ↓                          │
          │  ads_customer_profile  ads_daily_report            │
          └───────────────────────────────────────────────────┘
                                 ▲                              
                                 │  ClickHouse Spark Connector 
                                 │  （JDBC，分布式写）          
    ┌────────────────────────────┴────────────────────────────┐
    │                PySpark（本地模式）                        │
    │   read → pipelines（读→纯函数加工→校验→写）→ ClickHouse │
    └────────────────────────────┬────────────────────────────┘
                                 │ 读 Parquet                   
    ┌────────────────────────────▼────────────────────────────┐
    │          MinIO（数据湖，原始数据由 generator/ 造）         │
    │  fact 桶： fact_transaction.parquet       （历史交易事实）│
    │  dim 桶：  dim_customer.parquet           （客户维表）    │
    │           dim_merchant.parquet            （商户维表）    │
    └─────────────────────────────────────────────────────────┘
```

**数据流**（generator 负责造数，batch 负责加工）：

```
【generator/ 目录 —— 数据生产，不属于 batch】
build_dimensions.py    ──→ MinIO dim 桶   造维表（先生成）
build_transactions.py  ──→ MinIO fact 桶  批量造历史交易（ID 取自维表池）
                                   ↓
【batch/ 目录 —— Spark 分层加工，一张表 = src/pipelines/ 下一个模块】
步骤5 ods_transaction.py    fact/dim → ODS        原样落地 ClickHouse
步骤5 dwd_transaction.py    ODS → 清洗 + JOIN 维表打宽 → DWD
步骤5 dws_customer_daily.py  DWD → 按客户+天聚合 → DWS
步骤5 dws_merchant_daily.py  DWD → 按商户+天聚合 → DWS
步骤5 ads_customer_profile.py / ads_daily_report.py  DWS → 画像/大盘 → ADS
步骤7 sync_dim_redis.py     ClickHouse dim_* ──→ Redis  （实时 Flink 查 Redis 打宽用）
```

> ⚠️ 边界约定：**generator 负责造数（写 MinIO），batch 只读数不造数**。这符合真实数仓"主数据先行"的做法——维表作为主数据先生成，交易事实引用维表已有的 ID，天然保证 referential integrity（不会出现 JOIN 不上的孤儿 ID）。因此 batch 里没有"复制桶""从交易去重造维度"这类作业。
>
> 📦 **调度单元约定**：`src/pipelines/` 下**一个文件 = 一张目标表 = 一个可被 Airflow 调度的任务**。每个模块暴露 `run(dt)` 入口（内部自己完成 读→加工→校验→写），模块内的 `clean/aggregate/build` 是纯函数（DF→DF）供单测。Airflow 管表与表之间的依赖（ODS→DWD→DWS），表内部流程是模块自己的代码。

***

## 目录结构

```
batch/
├── Dockerfile                   # 构建镜像（基于 apache/spark；构建时下载 connector jar + 装纯 Python 依赖）
├── .dockerignore                # 构建上下文排除（.pytest_cache / __pycache__ 等）
├── requirements.txt              # 纯 Python 依赖（pyspark 镜像自带，不列入）
├── jars/                         # 空目录！connector jar 在镜像构建时从 Maven 下载进 /opt/spark/jars/，不提交二进制
├── README.md                     # 本文档
│
├── config/                       # 配置层
│   └── settings.py               #   Pydantic Settings（MinIO/ClickHouse/Redis，env var 覆盖）
│
├── src/                          # 核心层
│   ├── spark.py                  #   SparkSession 工厂（统一配 MinIO S3A + ClickHouse Catalog + Python worker）
│   ├── schemas.py                #   StructType（与 generator/schema.py 对齐）
│   ├── quality.py                #   数据质量校验（行数对比 / NULL 率 / 聚合比）
│   │
│   ├── io/                       #   读写层：换数据源只改这里
│   │   ├── minio_reader.py       #     读 MinIO Parquet（fact 交易 / dim 维表）
│   │   ├── clickhouse_admin.py   #     ClickHouse DDL：ensure_table 按 clickhouse/ddl/{表名}.sql 幂等建表
│   │   ├── clickhouse_writer.py  #     写 ClickHouse：overwrite_partition（事实表按天覆盖）/ overwrite_table（维表全量）
│   │   └── clickhouse_reader.py  #     读 ClickHouse（DWS/ADS 回读上层表用）
│   │
│   └── pipelines/                #   表任务层：一个文件 = 一张目标表 = 一个调度任务
│       ├── ods_transaction.py    #     ODS：fact → ods_transaction（原样落地）
│       ├── dwd_transaction.py    #     DWD：清洗 + JOIN 维表打宽 → dwd_transaction_offline
│       │                           #     （内含纯函数 clean/enrich_customer/enrich_merchant）
│       ├── dws_customer_daily.py #     DWS：按客户+天聚合 → dws_customer_daily
│       ├── dws_merchant_daily.py #     DWS：按商户+天聚合 → dws_merchant_daily
│       ├── ads_customer_profile.py#   ADS：客户画像 → ads_customer_profile
│       ├── ads_daily_report.py   #     ADS：每日大盘 → ads_daily_report
│       └── sync_dim_redis.py     #     ClickHouse 维表 → Redis（checklist 步骤 7）
│
└── tests/                        # 测试层
    ├── conftest.py               #   SparkSession fixture（整个 session 共用一个）
    ├── test_pipelines.py         #   表任务纯函数单测（造假 DF，不连 MinIO/ClickHouse）
    └── smoke_minio.py            #   冒烟测试：spark-submit 跑，验证 SparkSession 能连 MinIO
```

### 分层设计原则

| 层                | 为什么单独存在                                                  | 改了会影响什么                         |
| ---------------- | -------------------------------------------------------- | ------------------------------- |
| `config/`        | 连接参数不硬编码，env var 一切换就是 dev/prod                          | 每个任务不用再写死 `localhost:9000`      |
| `src/spark.py`   | SparkSession 创建逻辑只写一次，配置统一                               | 每个任务不用各自 `.config(...)`         |
| `src/io/`        | 读写抽象，换数据源（MinIO→S3）只改 reader/writer                      | 读写逻辑和业务逻辑解耦                     |
| `src/pipelines/` | **一张表一个模块**，内含 `run(dt)` 完整流程（读→加工→校验→写），是 Airflow 的调度单元 | 加表 = 加文件；某张表逻辑改动只动一个文件          |
| `src/quality.py` | 每层加工后校验，防止脏数据静默流入下游                                      | DWD 打宽全 NULL 能提前发现，不是 DWS 对不上才找 |
| `tests/`         | pipelines 里的纯函数可脱离集群单测（造假 DF）                            | 改聚合逻辑不用跑全链路，pytest 就行           |

> pipelines 模块里分两部分：**纯业务函数**（`clean`/`aggregate`/`build`，DF→DF，可单测）和 **`run(dt)`** **入口**（建 Spark、调 io 读写、调 quality，给 Airflow/命令行调用）。这是"一个文件管一张表"和"业务逻辑可测"两个目标的折中，也是生产里 Spark 任务的常见写法。

***

## 环境准备

### 1. 启动基础设施 + Spark 容器（docker compose 起）

```bash
# 在项目根目录执行
docker compose -f ./deploy/docker-compose.yml up -d --build spark
# 同时起依赖服务（如未启动）
docker compose -f ./deploy/docker-compose.yml up -d minio clickhouse redis
```

Spark 容器基于 `apache/spark:4.2.0` 镜像，自带 Java 17 + Python + pyspark。Dockerfile 在构建时做两件事（新机器 `git clone` 后无需手动准备任何 jar）：

1. **下载 connector jar 进 `/opt/spark/jars/`**：ClickHouse connector（curl 固化）+ MinIO S3A 依赖（`--packages` 解析，已裁剪掉 AWS 全家桶 bundle，约 48MB）。spark-submit 启动时自动加载，命令行不用再带 `--jars`/`--packages`。
2. **装纯 Python 依赖**：clickhouse-connect、redis、pydantic-settings 等（requirements.txt）。

> 依赖怎么裁剪、jar 为什么放 `/opt/spark/jars/`，见 [Maven 依赖裁剪手册](../docs/knowledge/maven/v3-maven-dependency-trimming.md)。

### 2. 连接配置：默认值直接用 Docker 服务名

本项目已纯 Docker 化，`config/settings.py` 里连接地址的**默认值直接写 Docker 服务名**，容器内自动解析，compose 不需要注入地址：

```python
# config/settings.py
minio_endpoint: str = "http://minio:9000"   # 默认值就是 Docker 服务名
clickhouse_host: str = "clickhouse"
redis_host: str = "redis"
model_config = {"env_prefix": "BATCH_"}      # 保留：以后换环境可用 BATCH_* 环境变量覆盖
```

docker-compose.yml 的 `environment` 里**只保留 `PYTHONPATH`**（让 Python 能 import 模块），不再注入任何 `BATCH_*` 地址：

```yaml
environment:
  PYTHONPATH: /opt/jobs    # 仅此一个；连接地址走 settings.py 默认值
```

> 机制（`env_prefix` 如何映射、为什么默认值能直接写服务名）见 [BaseSettings 手册](../docs/knowledge/pydantic/v2-basesettings.md)。

***

## 执行方法

### 步骤 0：准备原始数据（在 generator/ 目录，不在 batch）

batch 只读数不造数。跑批前先确保 MinIO 的 dim / fact 桶有数据：

```bash
cd ../generator
source .venv/bin/activate          # generator 有自己的 venv
python build_dimensions.py         # 写 MinIO dim 桶（维表，先生成）
python build_transactions.py       # 批量造历史交易 → 写 MinIO fact 桶（默认 10000 条）
```

产出：`dim/dim_customer.parquet`、`dim/dim_merchant.parquet`、`fact/fact_transaction.parquet`。

### 步骤 3：PySpark 读 MinIO（冒烟验证）

在 Spark 容器里验证 SparkSession 能连 MinIO（connector jar 已在镜像里，命令行不带任何 `--jars`）：

```bash
docker compose -f ./deploy/docker-compose.yml exec -T spark \
  /opt/spark/bin/spark-submit --master local[*] tests/smoke_minio.py
```

预期输出 `交易行数: 100000`、`客户行数: 10000`、`=== 冒烟测试通过 ===`。脚本见 [tests/smoke_minio.py](tests/smoke_minio.py)。

### 步骤 4：ClickHouse 建表（DDL 在 clickhouse/ddl/，writer 写入前也会幂等自动建）

```bash
# 方式一：在 Spark 容器里批量执行整个目录（纯 Python 走 HTTP 8123，不起 Spark JVM，用 python3）
# DDL 目录由 compose 挂载到 /opt/clickhouse/ddl
docker compose -f ./deploy/docker-compose.yml exec -T spark \
  python3 -m src.io.clickhouse_admin /opt/clickhouse/ddl
```

```sql
-- 方式二：手动在 clickhouse-client / 控制台里执行 clickhouse/ddl/ 下的脚本：
-- dim_customer_offline、dim_merchant_offline、ods_customer、ods_merchant、ods_transaction、
-- dwd_transaction_offline、dws_customer_daily、dws_merchant_daily、
-- ads_customer_profile、ads_daily_report、ads_merchant_top10_daily（共 11 张）
```

> writer 每次写入前会调 `ensure_table()`（按 `clickhouse/ddl/{表名}.sql` 幂等 CREATE TABLE IF NOT EXISTS），
> 所以步骤 4 可省略；手动预建只是为了首次跑批前先确认 DDL 没问题。

### 步骤 5：分层加工 → ClickHouse

按顺序一层层跑，每层任务内部都有 quality 校验。在 Spark 容器里执行：

```bash
DT=2026-09-04
COMPOSE="docker compose -f ./deploy/docker-compose.yml exec -T spark"

$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/ods_transaction.py $DT    # ODS：原样落地
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/dwd_transaction.py $DT    # DWD：清洗 + JOIN 维表打宽
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/dws_customer_daily.py $DT # DWS：按客户+天聚合
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/dws_merchant_daily.py $DT # DWS：按商户+天聚合
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/ads_customer_profile.py $DT  # ADS：客户画像
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/ads_daily_report.py $DT      # ADS：每日大盘
```

DWD 任务模块的结构（dwd\_transaction.py，一文件一表）：

```python
# 纯业务函数（DF→DF，可单测）
def clean(df): ...
def enrich_customer(df, dim_customer): ...
def enrich_merchant(df, dim_merchant): ...

# 任务入口（Airflow / 命令行调这个）
def run(dt: str):
    spark = get_spark_session(app_name=f"dwd_transaction_{dt}")
    raw = read_from_clickhouse(spark, "ods_transaction", dt)        # io 层：读 ODS 当天分区
    dim_customer = read_from_clickhouse(spark, "dim_customer_offline")   # io 层：读维表（全表）
    dim_merchant = read_from_clickhouse(spark, "dim_merchant_offline")

    cleaned = clean(raw)                                     # 纯函数
    enriched = enrich_customer(cleaned, dim_customer)        # 纯函数
    enriched = enrich_merchant(enriched, dim_merchant)       # 纯函数

    check_dwd(raw, enriched)                                 # 质量校验（写入前）
    overwrite_partition_to_clickhouse(spark, enriched, "dwd_transaction_offline", dt)  # io 层：覆盖当天分区
    spark.stop()
```

> 只有 ODS 层（ods\_transaction / ods\_customer / ods\_merchant）直接读 MinIO Parquet；
> DWD 及以上各层都从 ClickHouse 回读上一层结果，保证层与层解耦。

### 步骤 7：ClickHouse 维表 → Redis

实时 Flink 打宽需要 Redis 里有维度数据（纯 Python，不起 Spark JVM，用 python3）：

```bash
docker compose -f ./deploy/docker-compose.yml exec -T spark \
  python3 -m src.pipelines.sync_dim_redis
# key: dim:customer:{customer_id} / dim:merchant:{merchant_id}
# value: Hash 存属性
# 这是 T+1 批量同步，不是实时
```

### 一条串行跑全链路

```bash
DT=2026-09-04
COMPOSE="docker compose -f ./deploy/docker-compose.yml exec -T spark"

$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/ods_transaction.py $DT && \
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/dwd_transaction.py $DT && \
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/dws_customer_daily.py $DT && \
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/dws_merchant_daily.py $DT && \
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/ads_customer_profile.py $DT && \
$COMPOSE /opt/spark/bin/spark-submit --master local[*] src/pipelines/ads_daily_report.py $DT && \
echo "全链路完成"
```

V3/V4 上 Airflow 后，**每个 pipelines 模块就是一个 task**（BashOperator 跑模块，或 PythonOperator 直接 import `run` 并传入业务日期 `{{ ds }}`），任务间依赖在 DAG 里声明：

```python
# dags/batch_daily.py（未来 V3/V4，不在 batch 目录内）
ods >> dwd >> [dws_customer, dws_merchant] >> [ads_profile, ads_report] >> sync_redis
```

***

## 单测

单测在 Spark 容器里跑。pytest 用 `python3`，但容器里 `python3` 默认不带 pyspark（pyspark 在 spark-submit 的 classpath 里），要手动把 pyspark.zip / py4j.zip 加进 `PYTHONPATH`：

```bash
# pytest Dockerfile 故意不装（避免污染生产镜像），先临时装；再带上 pyspark 跑
docker compose -f ./deploy/docker-compose.yml exec -T spark bash -c \
  "pip install pytest && \
   PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:\$PYTHONPATH \
   python3 -m pytest tests/ -q"
```

> 跑批作业用 `spark-submit`（自动配好 pyspark + connector jar）；pytest 不走 spark-submit，所以要手动补 pyspark 路径。

测什么：pipelines 模块里的**纯业务函数**（clean/aggregate/build），不用连 MinIO/ClickHouse，造假 DF 进来就可以。注意 `run()` 含 IO 不在这里测（那是集成测试）。示例：

```python
def test_clean_filters_negative_amount(spark):
    df = spark.createDataFrame([(-1,"C1"),(100,"C2")], ["amount","customer_id"])
    from src.pipelines.dwd_transaction import clean
    assert clean(df).count() == 1
```

***

## 生产架构惯例对照

这张卡用来快速判断"当前实现是不是行业内成熟的生产做法"。共 15 条，覆盖 7 大领域。

### 一、代码分层 & 可测试性

| # | 生产架构惯例                                                            | 当前实现                                                                            | 是否符合 |
| - | ----------------------------------------------------------------- | ------------------------------------------------------------------------------- | ---- |
| 1 | **IO 与业务逻辑解耦**：读写抽象（Reader/Writer）独立，业务函数只处理 DF，换数据源只改 io 层       | `src/io/`（MinIO/ClickHouse 读写）与 pipelines 里的纯业务函数（clean/aggregate）分离，纯函数不碰 IO   | ✅    |
| 2 | **一张表一个调度任务**：任务模块自己完成 读→加工→校验→写，Airflow 以表为单元调度                  | `src/pipelines/` 一个文件 = 一张目标表 = 一个任务，内含 `run(dt)` 入口；Airflow 直接调 `run(dt)`      | ✅    |
| 3 | **业务逻辑可脱离集群单测**：纯函数不依赖 MinIO / ClickHouse，造假 DF 即可跑 pytest        | pipelines 模块内的 clean/aggregate/build 是纯函数，tests/ 造假 DF 测，`run()` 含 IO 不参与单测     | ✅    |
| 4 | **SparkSession 单例工厂**：配置（S3A 连接、ClickHouse catalog、Python worker）集中在一处，任务不用重复写 | `src/spark.py` 统一 builder.config(...)；connector jar 由镜像 `/opt/spark/jars/` 自动加载，不在代码里配 | ✅    |

### 二、配置 & 环境管理

| # | 生产架构惯例                                                               | 当前实现                                                                                    | 是否符合 |
| - | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ---- |
| 5 | **连接参数不硬编码**：地址/账号/密码放配置或 env var，不在代码里写死                          | `config/settings.py` 用 Pydantic Settings；默认值直接写 Docker 服务名（`minio`/`clickhouse`/`redis`），保留 `BATCH_*` 环境变量覆盖能力 | ✅    |
| 6 | **环境配置一键就绪**：不要求用户记 export 命令，容器启动即可跑                               | 连接地址走 settings.py 默认值（容器网络服务名直接解析），compose 只注入 `PYTHONPATH`；connector jar 构建时固化进镜像，`docker compose up` 即就绪 | ✅    |
| 7 | **dev / prod 切换不碰代码**：环境前缀隔离（`BATCH_*`），切环境就是切环境变量                 | `env_prefix="BATCH_"` 保留，换 K8s/其他环境时注入 `BATCH_MINIO_ENDPOINT` 等即可覆盖默认值，不改 settings.py | ✅    |

### 三、数仓分层 & 数据质量

| #  | 生产架构惯例                                                                | 当前实现                                                                                                                                                                            | 是否符合  |
| -- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| 8  | **每层独立任务、支持单独重跑**：ODS / DWD / DWS / ADS 各一个表任务 + 业务日期参数，一层挂了不需要从第一层重来 | `src/pipelines/` 下 7 个表任务各自独立，都接收 `dt=YYYY-MM-DD`，可单独跑任意一张表                                                                                                                     | ✅     |
| 9  | **每层加工前有前置依赖检查**：跑 DWD 前确认 ODS 当天有数据，跑 DWS 前确认 DWD 当天有数据              | 未实现（当前 run() 直接读，没分区/数据存在性检查）；后续补                                                                                                                                               | ⚠️ 待补 |
| 10 | **质量在写入前校验**（写入即最终）：行数对比 / NULL 率 / ID 完整性，脏数据发现于写入前而非下游聚合发现时         | `src/quality.py`：`check_ods` 行数、`check_dwd` NULL 率、`check_dws` 聚合比；在各表任务调 `overwrite_partition_to_clickhouse()` **之前**校验                                                        | ✅     |
| 11 | **分层输出幂等**：同一 dt 跑多次不产生重复数据（覆盖写 / upsert）                             | writer 两个入口：事实表 `overwrite_partition_to_clickhouse`（`df.writeTo(t).overwrite(dt=当天)`，connector 集群侧先删后插，重跑当天不重复）；维表 `overwrite_table_to_clickhouse`（`overwrite(lit(True))` 整表覆盖） | ✅     |

### 四、调度 & 运维

| #  | 生产架构惯例                                                        | 当前实现                                                                                                 | 是否符合           |
| -- | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | -------------- |
| 12 | **任务接收业务日期参数**：不写死"今天"或"昨天"，由 Airflow 传入 `{{ ds }}`，便于重跑历史和回溯 | 所有表任务的 `run(dt)` 接收日期参数，`__main__` 从 `sys.argv[1]` 取，默认值仅用于手动验证                                      | ✅              |
| 13 | **每个任务有明确的 exit code / 异常传递**：失败即非 0 退出码，调度系统能判定失败触发告警        | 未专门处理异常（Python 默认未捕获异常返回 exit code 1，基本可用）；生产级建议加日志化异常捕获 + traceback                                 | ⚠️ 基础 OK，生产级可加 |
| 14 | **任务可声明成 DAG**：依赖关系清晰（ODS→DWD→DWS→ADS），任务粒度=表粒度，Airflow 迁移零改动 | Spark 表任务包成 BashOperator 跑 `spark-submit src/pipelines/xxx.py {{ds}}`；纯 Python 任务（sync_dim_redis）用 `python3 -m`，依赖在 DAG 里声明 | ✅              |

### 五、合规 & 可追溯

| #  | 生产架构惯例                                | 当前实现                                                                                                                                                                 | 是否符合            |
| -- | ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| 15 | **输入输出有明确血缘**：哪个任务读了哪张表哪个日期、写了哪张表有据可查 | 每个表任务开头 `read_minio()`/`read_from_clickhouse()` 和结尾 `overwrite_partition/table_to_clickhouse(table=...)` 形成直观血缘（模块内常量 TARGET\_TABLE/SOURCE\_TABLE）；生产级可接 OpenLineage | ⚠️ 基础 OK，可接血缘工具 |

### 符合度小结

```
✅ 完全符合（12 条）：分层、单测、配置、一键环境、独立重跑、质量前置、幂等写入、调度参数化...
⚠️ 有基础 / 待落地（3 条）：前置依赖检查、异常日志化、血缘采集
    → 血缘留到 V3/V4 Airflow 阶段再上
```

***

## 关键约定

| 约定                                         | 为什么                                         |
| ------------------------------------------ | ------------------------------------------- |
| 维表是主数据，**由 generator 先行造好**；交易只引用维表已有 ID   | 维表先有 ID 池，交易从池中选，不会出现 JOIN 不上的孤儿 ID         |
| pipelines 里的业务函数是**纯函数**：只拿 DF，只回 DF，不碰 IO | 才能单测；不然改一行要跑全链路                             |
| 一个模块管**一张目标表**，`run(dt)` 里自己读→加工→校验→写      | Airflow 以表为调度单元；加表=加文件，单表重跑互不影响             |
| `event_time` 截到毫秒                          | Flink JSON 只认 3 位微秒，6 位会解析失败                |
| 金额 `Decimal`，写 Parquet 保留精度                | Float 会有 0.1 + 0.2 = 0.30000000000000004 问题 |
| `spark.pyspark.python` 强制用容器自带 Python      | Spark 镜像里 Python 版本和 pyspark 绑定，强制指定防止版本不一致 |
| quality 校验在 **写入之前** 调                     | 脏数据进了下游再发现，代价是重新跑整个链路                       |

***

## Checklist 对应关系

| 文件 / 模块                                                         | v2.md checklist 步骤                          |
| --------------------------------------------------------------- | ------------------------------------------- |
| `generator/build_dimensions.py`                                 | 步骤 2 前置：造维表 → MinIO dim 桶（主数据先行）            |
| `generator/build_transactions.py`                               | 步骤 1：批量造历史交易 → MinIO fact 桶                 |
| `src/spark.py` + `src/io/minio_reader.py`                       | 步骤 3：PySpark 读 MinIO                        |
| `clickhouse/ddl/`（上级目录）                                         | 步骤 4：建分层表（writer 写入前也会 ensure\_table 幂等自动建） |
| `src/pipelines/ods_transaction.py`                              | 步骤 5：ODS → ClickHouse                       |
| `src/pipelines/dwd_transaction.py`                              | 步骤 5：DWD 清洗打宽 → ClickHouse                  |
| `src/pipelines/dws_customer_daily.py` / `dws_merchant_daily.py` | 步骤 5：DWS 聚合 → ClickHouse                    |
| `src/pipelines/ads_customer_profile.py` / `ads_daily_report.py` | 步骤 5：ADS 画像/报表 → ClickHouse                 |
| `src/pipelines/sync_dim_redis.py`                               | 步骤 7：ClickHouse 维表 → Redis                  |

> 说明：checklist 步骤 1/2 原设计的"造历史/从交易去重造维度"由 generator 目录承担（主数据先行，维表先生成、交易引用维表 ID），batch 只做步骤 3\~7 的加工。

