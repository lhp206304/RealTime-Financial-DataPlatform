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

## V1 接口清单

```text
GET /transactions/realtime          # 实时交易大盘（读 ADS 表）
GET /customers/{id}/statistics      # 单个客户的实时统计
GET /health                         # 健康检查
```

> 更多接口（风险、商户分析）后续阶段再加。

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
