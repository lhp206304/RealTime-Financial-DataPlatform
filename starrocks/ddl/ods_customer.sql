-- ODS 客户维度贴源表：MinIO dim 桶 dim_customer.parquet 原样落地
--
-- 模型选择：Duplicate Key（明细模型）
--   ODS 贴源不做去重/聚合，schema 与 Parquet 源文件严格一致（4 列，无 dt）；
--   幂等由写入侧保证（writer 先 TRUNCATE 再 append，整表重刷）。
-- 分区：不分区。维表全量整刷，无业务日期分区列。
-- 分桶：按 customer_id HASH，与下游 DIM/DWD JOIN 键一致。
CREATE TABLE IF NOT EXISTS finance.ods_customer (
    customer_id    VARCHAR(32)  NOT NULL  COMMENT "客户ID",
    level          VARCHAR(8)   NOT NULL  COMMENT "客户等级：V1/V2/V3/V4（CustomerLevel 枚举）",
    region         VARCHAR(32)  NOT NULL  COMMENT "注册地",
    register_time  DATETIME     NOT NULL  COMMENT "开户时间"
)
DUPLICATE KEY (customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
