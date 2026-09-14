# deploy —— 本地环境 (Docker Compose)

一键起全部基础设施：

```bash
docker compose up -d              # 全部服务
docker compose up -d minio clickhouse redis   # 只跑离线批链路依赖
```

## 服务清单（docker-compose.yml）

| 服务 | 用途 | 关键端口（宿主机） |
|---|---|---|
| `broker` | Kafka（KRaft 模式，免 Zookeeper） | 9092 |
| `init-kafka` | 一次性任务：建 `transaction` topic（3 分区） | - |
| `starrocks` | StarRocks（FE+BE allin1），实时链路 OLAP | 8030(FE HTTP) / 9030(MySQL 协议) |
| `jobmanager` / `taskmanager` | Flink 1.20（Java 17），消费 Kafka 写 StarRocks | 8081(Web UI) |
| `minio` | S3 兼容对象存储，generator 写 Parquet、batch 读 | 9000(API) / 9001(控制台) |
| `redis` | 维表缓存，Flink Lookup Join 打宽用 | 6379 |
| `clickhouse` | ClickHouse，离线批链路 OLAP | 8123(HTTP) / 9002(Native) |
| `spark-master` / `spark-worker` | Spark Standalone 集群，Airflow 提交 PySpark 作业 | 8080(Master UI) / 7077(Master RPC) |
| `postgres` | Airflow 元数据库 | - |
| `airflow-init` | 一次性：建表 + 建 admin 用户 | - |
| `airflow-webserver` | Airflow Web UI | 8088 |
| `airflow-scheduler` | Airflow 调度器（LocalExecutor，任务在此容器执行） | - |

## 启动顺序与健康检查

数据服务（minio / redis / clickhouse / spark-master）均配置了 healthcheck：

| 服务 | 探活方式 |
|---|---|
| minio | 读 `/proc/net/tcp` 检查 9000 端口 LISTEN（镜像极简无 curl） |
| redis | `redis-cli ping` |
| clickhouse | `clickhouse-client --query 'SELECT 1'` |
| spark-master | `curl http://localhost:8080/` |

`airflow-scheduler` 的 `depends_on` 声明了 `condition: service_healthy`，必须等上述 4 个服务全部健康后才启动，避免服务未就绪导致任务失败。

## Airflow 日志

`airflow-log` 卷由 webserver 和 scheduler 共享挂载到 `/opt/airflow/logs`，webserver 直接读本地日志文件，绕过 Airflow 2.9 log server 的 `DetachedInstanceError` bug（scheduler 容器 ID 不固定导致 webserver 拉取日志失败）。

## 提示

- 容器间用 service 名互相访问（如 `minio:9000`、`starrocks:9030`），端口映射给宿主机
- StarRocks 资源占用大，本地内存留足
- Flink connector jar（Kafka / StarRocks）直接放在 `/opt/flink/lib`，见 compose 卷挂载
- generator / batch / shared 目录挂载进 airflow-scheduler 容器，BashOperator / SparkSubmitOperator 直接调用
