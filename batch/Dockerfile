# PySpark 批任务运行环境：镜像自带 Spark 4.x + Java 17 + Python，无需另装
# 一键构建：docker compose -f ./deploy/docker-compose.yml up -d --build spark
# 依赖 jar 全部在构建时从 Maven 中央仓库下载，新机器 git clone 后无需手动准备任何 jar
FROM apache/spark:4.2.0

USER root

# ---- 1) ClickHouse Spark Connector（shaded fat jar，已内含依赖，直接 curl 固化）----
# spark-submit --packages 不支持 classifier（jdbc 的 all 包），故这两个都用 curl
RUN curl -fsSL -o /opt/spark/jars/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar \
      https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-4.0_2.13/0.10.0/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar \
 && curl -fsSL -o /opt/spark/jars/clickhouse-jdbc-0.9.5-all.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.5/clickhouse-jdbc-0.9.5-all.jar

# ---- 2) MinIO S3A 最小依赖（约 23MB）----
# hadoop-aws 3.5.0 与镜像自带 hadoop-client 3.5.0 版本对齐。
# 关键：排除 654MB 的 software.amazon.awssdk:bundle（AWS 全服务 uber jar，S3A 只用 S3/STS），
#       改用 s3 / sts / s3-transfer-manager 三个最小模块，传递依赖由 Maven 自动解析。
# 构建时跑一次 spark-submit 触发 --packages 下载，再把 ivy 缓存里的 jar 固化进 /opt/spark/jars。
RUN printf 'from pyspark.sql import SparkSession\nspark = SparkSession.builder.master("local[*]").getOrCreate()\nspark.stop()\n' > /tmp/warmup.py \
 && /opt/spark/bin/spark-submit --master local[*] \
      --packages "org.apache.hadoop:hadoop-aws:3.5.0,software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:1.3.1,software.amazon.awssdk:s3:2.35.4,software.amazon.awssdk:sts:2.35.4,software.amazon.awssdk:s3-transfer-manager:2.35.4" \
      --conf "spark.jars.excludes=software.amazon.awssdk:bundle" \
      /tmp/warmup.py \
 && find /root/.ivy2.5.2/cache -name '*.jar' -exec cp -n {} /opt/spark/jars/ \; \
 && rm -rf /root/.ivy2.5.2 /tmp/warmup.py \
 && rm -f /opt/spark/jars/netty-*-4.1.*.jar
# 上面这行不能省：AWS SDK v2 传递引入 netty 4.1.x，与 Spark 4.2 自带的 netty 4.2 同目录共存。
# classpath 按文件名排序时 4.1 的 netty-transport 排在 4.2 前面先加载，
# Spark 4.2 调用 4.2 才有的 SingleThreadEventLoop 构造方法 → NoSuchMethodError，
# SparkContext 都起不来。4.1 模块全部删掉，统一用 4.2（netty-nio-client 未被 S3A 同步链路加载）。

# ---- 3) Python 纯依赖（pyspark 镜像自带，不列入 requirements）----
COPY requirements.txt /tmp/requirements.txt
# 用清华源：容器不继承宿主机的 pip 全局配置
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r /tmp/requirements.txt
