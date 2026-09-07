-- DIM 商户维度表：ods_merchant 清洗后落地（dim_merchant.py），供 DWD 打宽 JOIN
--
-- 模型选择：Primary Key（主键模型）
--   维度是实体表，merchant_id 天然唯一；主键模型按主键 upsert/去重，
--   DWD JOIN 维表走点查更快，未来做拉链/SCD 也不用换模型。
-- 分区：不分区。维表数据量小、整表 TRUNCATE+append 重刷。
-- 分桶：按 merchant_id HASH，与事实表 JOIN 键一致。
CREATE TABLE IF NOT EXISTS finance.dim_merchant_offline (
    merchant_id   VARCHAR(32)  NOT NULL  COMMENT "商户ID",
    category      VARCHAR(32)  NOT NULL  COMMENT "商户类目",
    region        VARCHAR(32)  NOT NULL  COMMENT "商户所在地区",
    risk_level    VARCHAR(16)  NOT NULL  COMMENT "风险等级：LOW/MEDIUM/HIGH（RiskLevel 枚举）"
)
PRIMARY KEY (merchant_id)
DISTRIBUTED BY HASH(merchant_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
