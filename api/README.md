# api —— 查询服务 (FastAPI)

从 StarRocks 查实时指标，对外提供 REST API。

> Data Engineer 岗的事实标准：FastAPI + Pydantic，自带参数校验和 OpenAPI 文档（`/docs`）。

---

## 目录布局

```text
api/
├── app/                # 包代码（你写）
│   ├── __init__.py
│   ├── main.py         # FastAPI 实例 + 路由挂载
│   ├── routers/        # 各接口
│   ├── db.py           # StarRocks 连接（MySQL 协议）
│   └── schemas.py      # Pydantic 响应模型
├── tests/              # pytest（你写）
├── pyproject.toml
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

```bash
# 1. 激活虚拟环境
cd /Users/walterlee/Project/RealTime-Financial-DataPlatform/api
source .venv/bin/activate

# 2. 启动服务（二选一）
uvicorn app.main:app --reload --port 8000      # 方式 1：uvicorn 热重载（推荐开发用）
python -m app.main                             # 方式 2：直接跑（无热重载）

# 3. 测试接口
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
| Flink Job1 在跑（实时明细） | Web UI `localhost:8081` 有 Running Job |
| PySpark 离线链路跑过（离线画像） | `ads_customer_profile` 表有数据 |

---

## 你要实现的清单

### `main.py`
- [ ] 创建 FastAPI 实例，挂载 routers
- [ ] 启动/关闭事件（建连接池、优雅关闭）

### `db.py`
- [ ] 连 StarRocks（MySQL 协议，用 `PyMySQL` / `SQLAlchemy`）
- [ ] 连接池配置
- [ ] 查询带超时

### `routers/` + `schemas.py`
- [ ] 各接口：路径/查询参数校验（Pydantic） → 查库 → 返回响应模型
- [ ] 统一异常处理 + 日志

---

## 要练/要能讲清楚的知识点

| 主题 | 面试要能回答 |
|---|---|
| Pydantic | 请求/响应校验怎么做？ |
| async | FastAPI async 什么时候真正有用？ |
| 连接池 | 池大小怎么定？连接泄漏怎么防？ |
| 超时 | 慢查询怎么不拖垮服务？ |

---

## 建议实现顺序

1. 先 `/health` 跑通服务 + 看 `/docs`
2. 再接 StarRocks 查一张表返回 JSON
3. 最后补连接池 / 超时 / 异常处理 / 测试
