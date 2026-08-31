# deploy —— 本地环境 (Docker Compose)

V1 需要一键启动：**Kafka + Flink + StarRocks**。

```bash
docker compose up -d
```

---

## 你要实现的清单

`docker-compose.yml`，包含以下服务：

- [ ] **Kafka**（推荐 KRaft 模式，免 Zookeeper）
  - 暴露端口给 Go generator 和 Flink 连接
  - 启动后建 `transaction` topic（可脚本化，设 partition 数）
- [ ] **Flink**（JobManager + TaskManager）
  - 挂载 `../flink/` 目录方便提交作业
- [ ] **StarRocks**（FE + BE，可用官方 allin1 镜像先跑通）
  - 暴露 MySQL 协议端口给 Flink Sink 和 Go API

---

## 提示

- 先各服务单独跑通，再合并成一个 compose
- 注意容器间网络：用 service 名互相访问，端口映射给宿主机
- StarRocks 资源占用大，本地内存留足
