-- DIM 客户维度表：ods_customer 清洗后落地（dim_customer.py），供 DWD 打宽 JOIN
--
-- 模型选择：Primary Key（主键模型）
--   维度是实体表，customer_id 天然唯一；主键模型按主键 upsert/去重，
--   DWD JOIN 维表走点查更快，未来做拉链/SCD 也不用换模型。
-- 分区：不分区。维表数据量小、整表 TRUNCATE+append 重刷。
-- 分桶：按 customer_id HASH，与事实表 JOIN 键一致。
CREATE TABLE IF NOT EXISTS finance.dim_customer_offline (
    customer_id    VARCHAR(32)  NOT NULL  COMMENT "客户ID",
    level          VARCHAR(8)   NOT NULL  COMMENT "客户等级：V1/V2/V3/V4（CustomerLevel 枚举）",
    region         VARCHAR(32)  NOT NULL  COMMENT "注册地",
    register_time  DATETIME     NOT NULL  COMMENT "开户时间"
)
PRIMARY KEY (customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
