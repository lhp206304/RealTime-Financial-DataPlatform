# go-api —— 查询服务 (Go)

对外提供 REST API：StarRocks 查实时指标、ClickHouse 查离线指标。
作为 `api/`（FastAPI）的 Go 重写版本，接口契约完全一致。

> Chi 路由 + sqlx ORM + envconfig 配置，无 Swagger（Go 生态没有 FastAPI 那种自动生成 OpenAPI 的框架）。

---

## 目录布局

```text
go-api/
├── cmd/server/main.go       # 入口：读配置 → 建连接池 → 组装依赖 → 启动 :8001 → 优雅关闭
├── internal/
│   ├── config/              # 环境变量 → 结构体（envconfig）
│   ├── logger/              # slog 初始化
│   ├── middleware/          # 横切：请求日志、panic 恢复
│   ├── model/               # 共享结构体（transaction/customer）
│   ├── repository/          # 数据层：StarRocks（MySQL 协议）+ ClickHouse（HTTP）
│   ├── service/             # 业务层：拼 SQL、编排并发
│   └── handler/             # 接口层：参数解析 / 超时 / JSON 响应
├── Dockerfile               # 多阶段构建（golang:1.27-alpine → distroless/static）
├── .dockerignore
└── go.mod / go.sum
```

**分层依赖方向（铁律，只准上层 import 下层）：**

```
main → router → handler → service → repository → config
       ↓          ↓           ↓
    middleware   model      model
```

---

## 接口清单

```text
GET /health                              # 健康检查
GET /transactions?customer_id=&limit=    # 实时交易列表（StarRocks）
GET /customers/{customer_id}/full-profile # 流批汇聚：实时统计 + 离线画像（StarRocks + ClickHouse 并发）
```

| 接口 | 必填参数 | 数据源 |
|---|---|---|
| `/health` | 无 | 无 |
| `/transactions` | `customer_id`（缺失返回 400） | StarRocks `dwd_transaction_online` |
| `/customers/{id}/full-profile` | 路径参数 | StarRocks + ClickHouse（`errgroup` 并发） |

---

## 启动与测试

### 方式 1：Docker 容器（推荐，随 compose 一起起）

```bash
# 全量启动已包含 go-api；单独启动/重启：
cd deploy
docker compose up -d --build go-api
# 端口 8001，连接信息由 *conn-env 锚点注入（与 api/spark/airflow 共用同一套变量）
```

### 方式 2：本地 `go run`（开发调试）

```bash
cd go-api
go run ./cmd/server
# 会自动加载 ../deploy/.env（godotenv.Load 路径相对于 go-api/ 目录）
```

### 测试接口（两种方式相同）

```bash
curl localhost:8001/health
curl "localhost:8001/transactions?customer_id=1&limit=3"
curl localhost:8001/customers/1/full-profile
```

### 前提条件

| 依赖 | 状态 |
|---|---|
| StarRocks 容器在跑 | `docker ps` 看到 `starrocks` |
| ClickHouse 容器在跑 | `docker ps` 看到 `clickhouse` |

---

## 环境变量

go-api 通过 `envconfig` 从前缀为 `STARROCKS_` / `CLICKHOUSE_` 的环境变量读取连接信息，字段命名与 `deploy/docker-compose.yml` 的 `*conn-env` 锚点完全对齐，容器内直接 `<<: *conn-env` 注入。

### 连接信息（无默认值，缺失启动报错）

| 变量 | 说明 |
|---|---|
| `STARROCKS_HOST` / `STARROCKS_MYSQL_PORT` / `STARROCKS_USER` / `STARROCKS_PASSWORD` / `STARROCKS_DATABASE` | 实时链路 |
| `CLICKHOUSE_HOST` / `CLICKHOUSE_HTTP_PORT` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DATABASE` | 离线链路 |

### 代码级参数（有默认值）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `RUNTIME_QUERY_TIMEOUT` | `5s` | 单次查询超时（handler 层 ctx 控制） |
| `RUNTIME_SHUTDOWN_TIMEOUT` | `30s` | 优雅关闭等待上限 |

---

## 设计要点

| 主题 | 说明 |
|---|---|
| **双数据源** | StarRocks 走 `go-sql-driver/mysql` + `sqlx` 连接池查实时表；ClickHouse 走 `clickhouse-go/v2` HTTP 协议查离线表。`repository/` 包只做"建连接池 + 通用 QueryContext 封装"，**不硬编码任何表名**——SQL 全由 service 层决定 |
| **并发查询** | `/customers/{id}/full-profile` 同时查 StarRocks 和 ClickHouse，用 `errgroup.Group` 并发执行 → 总耗时取两条链路的最大值，不是相加 |
| **超时控制** | 每条请求 handler 层 `context.WithTimeout(..., 5s)`，底层 `SelectContext` 感知 ctx 取消提前返回，慢查询不拖垮服务 |
| **SQL 注入** | 外部输入全部走 `?` 参数占位符；唯一例外是 `LIMIT`——StarRocks 不支持参数化 LIMIT（Error 1064），直接拼接 int 且在 handler 层已校验为正整数，无注入面 |
| **优雅关闭** | `signal.NotifyContext` 监听 SIGINT/SIGTERM，收到信号先 `http.Server.Shutdown`（等已有请求收尾），超时后 `Close` 强制断连 |
| **容器化** | 多阶段构建：`golang:1.27-alpine` 编译 → `gcr.io/distroless/static-debian12` 运行。`CGO_ENABLED=0` 保证纯静态二进制能放进 distroless（distroless 无 glibc） |
| **健康检查** | distroless 运行镜像无 shell/curl，`docker-compose.yml` 无法写容器内 healthcheck——实际就绪靠启动日志确认（`server starting`） |
| **错误处理** | 业务错误只在 handler 记日志（handler 拿得到完整请求上下文）；service / repository 只 `return err`，不打日志 |

---

## 与 Python 版 api/ 的对照

| 维度 | `api/`（FastAPI） | `go-api/` |
|---|---|---|
| 路由 | FastAPI 自动注册 | Chi 手动挂载 |
| 参数校验 | Pydantic 自动 | 手动 `strconv.Atoi` + 判空 |
| 异步 | `async def` + 线程池跑同步驱动 | 全同步，单请求串行，靠 goroutine 扩并发 |
| ORM | SQLAlchemy + clickhouse-connect | sqlx（结构体自动映射）+ clickhouse-go/v2 |
| 依赖注入 | FastAPI Depends 自动 | 构造函数手动组装 |
| OpenAPI | 自动生成 `/docs` | 无 |
| 启动命令 | `uvicorn app.main:app` | `go run ./cmd/server` |
