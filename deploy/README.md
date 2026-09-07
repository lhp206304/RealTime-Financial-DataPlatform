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

## 提示

- 容器间用 service 名互相访问（如 `minio:9000`、`starrocks:9030`），端口映射给宿主机
- StarRocks 资源占用大，本地内存留足
- Flink connector jar（Kafka / StarRocks）直接放在 `/opt/flink/lib`，见 compose 卷挂载
