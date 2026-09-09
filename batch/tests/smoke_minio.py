"""冒烟测试：SparkSession + MinIO 连通性（不依赖 spark-submit，可被 python3 直接运行）。

运行：
  docker compose -f ./deploy/docker-compose.yml exec -T spark \
    /opt/spark/bin/spark-submit --master local[*] tests/smoke_minio.py
"""
from src.spark import get_spark_session
from src.io.minio_reader import read_minio


def main():
    spark = get_spark_session("smoke_test")
    try:
        print("=== 1. SparkSession 启动成功 ===")
        print("Spark version:", spark.version)

        # 2. 读 fact 桶交易数据
        txns = read_minio(spark, "fact", "fact_transaction")
        print("=== 2. MinIO fact_transaction 读成功 ===")
        print("交易行数:", txns.count())
        txns.printSchema()

        # 3. 读 dim 桶客户维表
        cust = read_minio(spark, "dim", "dim_customer")
        print("=== 3. MinIO dim_customer 读成功 ===")
        print("客户行数:", cust.count())
        cust.show(3)
        print("=== 冒烟测试通过 ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
