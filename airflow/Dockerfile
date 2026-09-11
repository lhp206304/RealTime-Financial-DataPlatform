# 方式一：官方 airflow + 装 Spark（标准做法）
FROM apache/airflow:2.9.3-python3.11

USER root

# 装 Java（Spark 依赖）
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 从已构建的 batch-spark 镜像复制 Spark（复用，不用重新下载）
COPY --from=batch-spark:4.2.0 /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin
# JAVA_HOME 不能硬编码 amd64：Apple Silicon 上目录是 java-17-openjdk-arm64
# 用通配符软链统一路径，两种架构都指向 /usr/lib/jvm/java-17
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow

# Airflow Spark provider（SparkSubmitOperator）
# provider 钉死 4.9.0：更高版本要求 Airflow 3.x，会把基础镜像的 2.9.3 静默升级坏掉。
# 必须 --no-deps：它的依赖声明了 pyspark，pip 会装 ~400MB 完整 Spark 发行版，
# 但本镜像只通过 spark-submit 调 /opt/spark（pyspark 已在里面），装了纯浪费还撑爆磁盘。
# grpcio-status 是 provider 的另一个小依赖（spark-connect 链路用），单独补上。
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --no-deps apache-airflow-providers-apache-spark==4.9.0 \
 && pip install --no-cache-dir grpcio-status \
 && pip install --no-cache-dir -r /tmp/requirements.txt