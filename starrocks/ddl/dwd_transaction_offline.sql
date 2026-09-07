-- DWD 交易明细宽表：ods_transaction 清洗 + JOIN dim_customer_offline / dim_merchant_offline 打宽
--
-- 模型选择：Duplicate Key（明细模型）
--   明细流水只 append、不去重；幂等由写入侧保证（writer 先 DELETE WHERE dt=当天 再 append）。
-- 分区：date_trunc('day', dt) 表达式分区，按天重跑只覆盖当天；dt 列随 ods_transaction 源表带入。
-- 分桶：按 customer_id HASH，客户维度分析/与 DWS 上游一致。
-- 打宽列（6 列）允许 NULL：LEFT JOIN 未命中时为空，由 quality 校验拦截。
-- 字段对齐 dwd_transaction.py：
--   enrich_customer → customer_level / customer_region / customer_register_time
--   enrich_merchant → merchant_category / merchant_region / merchant_risk_level
CREATE TABLE IF NOT EXISTS finance.dwd_transaction_offline (
    transaction_id           VARCHAR(64)     NOT NULL  COMMENT "交易ID",
    event_time               DATETIME        NOT NULL  COMMENT "事件时间（毫秒精度）",
    amount                   DECIMAL(18, 2)  NOT NULL  COMMENT "金额（DECIMAL 精确，不用浮点）",
    currency                 VARCHAR(8)      NOT NULL  COMMENT "币种：CNY/USD/EUR",
    customer_id              VARCHAR(32)     NOT NULL  COMMENT "客户ID",
    account_id               VARCHAR(32)     NOT NULL  COMMENT "账户ID",
    merchant_id              VARCHAR(32)     NOT NULL  COMMENT "商户ID",
    transaction_type         VARCHAR(16)     NOT NULL  COMMENT "交易类型：PAYMENT/REFUND",
    customer_level           VARCHAR(8)          NULL  COMMENT "客户等级（JOIN dim_customer_offline.level）",
    customer_region          VARCHAR(32)         NULL  COMMENT "客户注册地",
    customer_register_time   DATETIME            NULL  COMMENT "客户开户时间（JOIN dim_customer_offline.register_time）",
    merchant_category        VARCHAR(32)         NULL  COMMENT "商户类目（JOIN dim_merchant_offline.category）",
    merchant_region          VARCHAR(32)         NULL  COMMENT "商户地区",
    merchant_risk_level      VARCHAR(16)         NULL  COMMENT "商户风险等级 LOW/MEDIUM/HIGH（JOIN dim_merchant_offline.risk_level）",
    dt                       DATE            NOT NULL  COMMENT "业务日期分区列，随 ods_transaction 源表带入，全链路统一按 dt 分区"
)
DUPLICATE KEY (transaction_id, event_time)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
