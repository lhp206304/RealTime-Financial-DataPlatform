# api —— 查询服务 (FastAPI)

对外提供 REST API：StarRocks 查实时指标、ClickHouse 查离线指标。

> FastAPI + Pydantic，自带参数校验和 OpenAPI 文档（`/docs`）。

---

## 目录布局

```text
api/
├── app/
│   ├── __init__.py
│   ├── main.py         # FastAPI 实例 + 路由挂载 + 启动/关闭事件
│   ├── routers/        # 各接口（transactions / customers）
│   ├── db.py           # 双数据源：StarRocks（SQLAlchemy 连接池）+ ClickHouse
│   └── schemas.py      # Pydantic 响应模型
└── README.md
```

---

## V2 接口清单

```text
GET /health                              # 健康检查
GET /transactions/realtime               # 实时交易大盘（读 ADS 表）
GET /customers/{id}/statistics           # 单个客户的实时统计（实时链路）
GET /customers/{id}/full-profile         # 流批汇聚：实时统计 + 离线画像
```

## 启动与测试

### 方式 1：Docker 容器（推荐，随 compose 一起起）

```bash
# 全量启动时已包含 api；单独启动/重启：
docker compose -f ../deploy/docker-compose.yml up -d api
# 容器自带 healthcheck（探活 /health），端口 8000，连接信息由 conn-env 环境变量注入
```

### 方式 2：本地 venv（开发调试，热重载）

```bash
# 1. 激活虚拟环境
cd /Users/walterlee/Project/RealTime-Financial-DataPlatform/api
source .venv/bin/activate

# 2. 启动服务（二选一）
uvicorn app.main:app --reload --port 8000      # 方式 1：uvicorn 热重载（推荐开发用）
python -m app.main                             # 方式 2：直接跑（无热重载）
```

### 测试接口（两种方式相同）

```bash
# 浏览器打开 API 文档（自动生成）
open http://localhost:8000/docs

# 命令行测试
curl http://localhost:8000/health
curl http://localhost:8000/customers/C12050/statistics
curl http://localhost:8000/customers/C12050/full-profile
```

### 验证流批汇聚（步骤 12）

`GET /customers/{customer_id}/full-profile` 返回结构：

```json
{
  "customer_id": "C12050",
  "realtime_stats": [           // 来自 dwd_transaction_online（Flink 实时链路）
    {"transaction_type": "PAYMENT", "amount_sum": 5000.00, "transaction_count": 12},
    {"transaction_type": "REFUND", "amount_sum": 200.00, "transaction_count": 1}
  ],
  "offline_profile": {          // 来自 ads_customer_profile（PySpark 离线链路）
    "dt": "2026-09-05",
    "txn_count": 89,
    "total_amount": 125000.00,
    "amount_tier": "HIGH",
    "ma7_total_amount": 98000.00,
    "dod_amount_change": 5000.00
  }
}
```

| 字段 | 来源 | 链路 |
|---|---|---|
| `realtime_stats` | `dwd_transaction_online` | 实时（Flink Job1 写入） |
| `offline_profile` | `ads_customer_profile` | 离线（PySpark T+1 写入） |

> `offline_profile` 为 `null` = 离线链路还没跑（`ads_customer_profile` 无数据），不影响实时部分。

### 前提条件

| 依赖 | 状态 |
|---|---|
| StarRocks 容器在跑 | `docker ps` 看到 `starrocks` |
| ClickHouse 容器在跑（离线画像） | `docker ps` 看到 `clickhouse` |
| Flink Job1 在跑（实时明细） | Web UI `localhost:8081` 有 Running Job |
| PySpark 离线链路跑过（离线画像） | ClickHouse `ads_customer_profile` 表有数据 |

---

## 设计要点

| 主题 | 说明 |
|---|---|
| 双数据源 | StarRocks 走 SQLAlchemy + PyMySQL 连接池查实时表（`run_query()`）；ClickHouse 走 clickhouse-connect HTTP 查离线表（`run_query_clickhouse()`）。路由按表的来源选入口，`db.py` 内部把 `:name` 占位符自动转成 `%(name)s`，两边 SQL 写法统一 |
| 参数校验 | Pydantic 负责响应模型和 Query 参数（limit/customer_id）校验，非法参数在进路由前被拒 |
| 连接池 | StarRocks 侧 `pool_size=5` + `max_overflow=5` 常驻复用连接；`pool_recycle=3600` 防服务端断连，`pool_pre_ping=True` 借出前 ping 掉死连接；启动事件初始化、关闭事件 dispose |
| 超时 | 两条链路查询超时均 5 秒（PyMySQL `read_timeout` / clickhouse-connect `query_timeout`），慢查询不拖垮服务 |
| async | 路由用 `async def`，但 DB 驱动是同步的，查询在线程池里执行（FastAPI 默认行为）；当前 QPS 下够用，避免引入异步驱动复杂度 |
| SQL 注入 | 全部走命名参数绑定（`text(sql)` + params dict），不手工拼字符串 |
