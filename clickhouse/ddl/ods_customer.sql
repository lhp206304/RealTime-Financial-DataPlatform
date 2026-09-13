-- ODS 客户维度贴源表：MinIO dim 桶 dim_customer.parquet 原样落地
--
-- 引擎：ReplacingMergeTree（无分区维表，TRUNCATE 整刷，ORDER BY 唯一键去重）
-- 分区：不分区（维表全量整刷，数据量小）
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.ods_customer
(
    customer_id    String   NOT NULL COMMENT '客户ID',
    level          String   NOT NULL COMMENT '客户等级：V1/V2/V3/V4（CustomerLevel 枚举）',
    region         String   NOT NULL COMMENT '注册地',
    register_time  DateTime NOT NULL COMMENT '开户时间',
    status         String   NOT NULL COMMENT '状态：ACTIVE 存活 / DELETED 软删'
)
ENGINE = ReplacingMergeTree()
ORDER BY customer_id
SETTINGS index_granularity = 8192;
