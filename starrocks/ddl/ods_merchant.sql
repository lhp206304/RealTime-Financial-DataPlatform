-- ODS 商户维度贴源表：MinIO dim 桶 dim_merchant.parquet 原样落地
--
-- 模型选择：Duplicate Key（明细模型）
--   ODS 贴源不做去重/聚合，schema 与 Parquet 源文件严格一致（4 列，无 dt）；
--   幂等由写入侧保证（writer 先 TRUNCATE 再 append，整表重刷）。
-- 分区：不分区。维表全量整刷，无业务日期分区列。
-- 分桶：按 merchant_id HASH，与下游 DIM/DWD JOIN 键一致。
CREATE TABLE IF NOT EXISTS finance.ods_merchant (
    merchant_id   VARCHAR(32)  NOT NULL  COMMENT "商户ID",
    category      VARCHAR(32)  NOT NULL  COMMENT "商户类目",
    region        VARCHAR(32)  NOT NULL  COMMENT "商户所在地区",
    risk_level    VARCHAR(16)  NOT NULL  COMMENT "风险等级：LOW/MEDIUM/HIGH（RiskLevel 枚举）"
)
DUPLICATE KEY (merchant_id)
DISTRIBUTED BY HASH(merchant_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
