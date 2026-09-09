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
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Airflow Spark provider（SparkSubmitOperator）
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt