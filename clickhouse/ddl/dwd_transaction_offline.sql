-- DWD 交易明细宽表：ods_transaction 清洗 + JOIN dim_customer_offline / dim_merchant_offline 打宽
--
-- 引擎：MergeTree（明细流水只 append，写入侧 DELETE PARTITION 再 append 保证幂等）
-- 分区：toYYYYMM(dt)，按月分区
-- 排序键：(customer_id, dt, transaction_id) 支持客户维度查询裁剪
-- 打宽列（6 列）允许 NULL：LEFT JOIN 未命中时为空
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.dwd_transaction_offline
(
    transaction_id           String        NOT NULL COMMENT '交易ID',
    event_time               DateTime      NOT NULL COMMENT '事件时间（毫秒精度）',
    amount                   Decimal(18,2) NOT NULL COMMENT '金额（DECIMAL 精确，不用浮点）',
    currency                 String        NOT NULL COMMENT '币种：CNY/USD/EUR',
    customer_id              String        NOT NULL COMMENT '客户ID',
    account_id               String        NOT NULL COMMENT '账户ID',
    merchant_id              String        NOT NULL COMMENT '商户ID',
    transaction_type         String        NOT NULL COMMENT '交易类型：PAYMENT/REFUND',
    customer_level           String             NULL COMMENT '客户等级（JOIN dim_customer_offline.level）',
    customer_region          String             NULL COMMENT '客户注册地',
    customer_register_time   DateTime           NULL COMMENT '客户开户时间（JOIN dim_customer_offline.register_time）',
    merchant_category        String             NULL COMMENT '商户类目（JOIN dim_merchant_offline.category）',
    merchant_region          String             NULL COMMENT '商户地区',
    merchant_risk_level      String             NULL COMMENT '商户风险等级 LOW/MEDIUM/HIGH（JOIN dim_merchant_offline.risk_level）',
    dt                       Date          NOT NULL COMMENT '业务日期分区列，随 ods_transaction 源表带入，全链路统一按 dt 分区'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (customer_id, dt, transaction_id)
SETTINGS index_granularity = 8192;
