-- ODS 商户维度贴源表：MinIO dim 桶 dim_merchant.parquet 原样落地
--
-- 引擎：ReplacingMergeTree（无分区维表，TRUNCATE 整刷，ORDER BY 唯一键去重）
-- 分区：不分区（维表全量整刷，数据量小）
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.ods_merchant
(
    merchant_id   String   NOT NULL COMMENT '商户ID',
    category      String   NOT NULL COMMENT '商户类目',
    region        String   NOT NULL COMMENT '商户所在地区',
    risk_level    String   NOT NULL COMMENT '风险等级：LOW/MEDIUM/HIGH（RiskLevel 枚举）',
    status        String   NOT NULL COMMENT '状态：ACTIVE 存活 / DELETED 软删'
)
ENGINE = ReplacingMergeTree()
ORDER BY merchant_id
SETTINGS index_granularity = 8192;
