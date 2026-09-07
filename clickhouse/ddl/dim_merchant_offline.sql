-- DIM 商户维度表：ods_merchant 清洗后落地，供 DWD 打宽 JOIN
--
-- 引擎：ReplacingMergeTree（维表实体表，merchant_id 唯一，ORDER BY 去重）
-- 分区：不分区（维表数据量小、整表 TRUNCATE+append 重刷）
-- 查询时加 FINAL 强制去重：SELECT * FROM finance.dim_merchant_offline FINAL
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.dim_merchant_offline
(
    merchant_id   String   NOT NULL COMMENT '商户ID',
    category      String   NOT NULL COMMENT '商户类目',
    region        String   NOT NULL COMMENT '商户所在地区',
    risk_level    String   NOT NULL COMMENT '风险等级：LOW/MEDIUM/HIGH（RiskLevel 枚举）'
)
ENGINE = ReplacingMergeTree()
ORDER BY merchant_id
SETTINGS index_granularity = 8192;
