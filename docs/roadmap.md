# 分阶段路线 (Roadmap)

> 核心原则：**不要一次全做**。一个阶段跑通再进下一个。

---

## V1 —— 实时主链路 【当前阶段】

```text
Python Generator → Kafka → Flink → StarRocks → FastAPI
```

目标：端到端跑通。这是第一优先级。

---

## V2 —— 加入离线（流 + 批）

```text
R2 → PySpark → StarRocks
```

用 PySpark 处理历史数据、特征工程，与实时链路汇聚到 StarRocks。

---

## V3 —— Python / 工程化

补：Pydantic、pytest、logging、配置管理、异常处理、Docker 化。

---

## V4 —— 数据质量

独立 DQ 模块：NULL / 重复 / 非法金额 / 非法时间 / 缺失 ID / Schema 错误 + 质量报告。

---

## V5 —— AI / 算法

```text
历史数据 → PySpark 特征工程 → Scikit-learn → 风险模型 → Flink 实时风险评分
```

从 Isolation Forest 入手。重点是「数据 → 特征 → 模型 → 评价 → 应用」，不是数学推导。

---

## V6 —— Cloud

Cloudflare R2 + CI/CD (GitHub Actions) + 云部署。最后做。

---

## V7 —— Go 高并发练习（加分项）

主链路跑通后，用 **Go 重写 generator 和 API**，专门练高并发：

```text
Go Generator → Kafka        （goroutine + channel 并发生产）
StarRocks → Go API          （连接池 + context 超时 + 优雅关闭）
```

- 不改变主链路（Python 版仍是主体），Go 版作为并行实现
- 目的：简历形成 **Python(主) + Go(加分)** 双标签，补 Go 高并发经验
- 要练：goroutine 数量控制、channel 背压、连接池、context 取消、优雅退出

---

## 简历技术栈（最终，控制范围）

核心标签：**Python / SQL / Kafka / Flink / Spark / PySpark / StarRocks / FastAPI / Docker / Cloud**

Go 作为**可选加分标签**（做完 V7 再加）。

> 简历里不要全堆上，突出核心链路即可。
