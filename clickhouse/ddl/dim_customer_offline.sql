-- DIM 客户维度表：ods_customer 清洗后落地，供 DWD 打宽 JOIN
--
-- 引擎：ReplacingMergeTree（维表实体表，customer_id 唯一，ORDER BY 去重）
-- 分区：不分区（维表数据量小、整表 TRUNCATE+append 重刷）
-- 查询时加 FINAL 强制去重：SELECT * FROM finance.dim_customer_offline FINAL
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.dim_customer_offline
(
    customer_id    String   NOT NULL COMMENT '客户ID',
    level          String   NOT NULL COMMENT '客户等级：V1/V2/V3/V4（CustomerLevel 枚举）',
    region         String   NOT NULL COMMENT '注册地',
    register_time  DateTime NOT NULL COMMENT '开户时间'
)
ENGINE = ReplacingMergeTree()
ORDER BY customer_id
SETTINGS index_granularity = 8192;
