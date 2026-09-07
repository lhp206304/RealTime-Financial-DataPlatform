-- ODS 交易明细表：MinIO fact 桶原始交易原样落地
--
-- 引擎：MergeTree（明细 append，写入侧 DELETE PARTITION 再 append 保证幂等）
-- 分区：toYYYYMM(dt)，按月分区（比按天分区目录数少，管理方便）
-- 排序键：(customer_id, dt, transaction_id) 支持客户维度查询裁剪
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.ods_transaction
(
    transaction_id    String        NOT NULL COMMENT '交易ID',
    event_time        DateTime      NOT NULL COMMENT '事件时间（毫秒精度）',
    amount            Decimal(18,2) NOT NULL COMMENT '金额（DECIMAL 精确，不用浮点）',
    currency          String        NOT NULL COMMENT '币种，如 CNY',
    customer_id       String        NOT NULL COMMENT '客户ID',
    account_id        String        NOT NULL COMMENT '账户ID',
    merchant_id       String        NOT NULL COMMENT '商户ID',
    transaction_type  String        NOT NULL COMMENT '交易类型：PAYMENT/REFUND 等',
    dt                Date          NOT NULL COMMENT '业务日期分区列 = to_date(event_time)，全链路统一按 dt 分区'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (customer_id, dt, transaction_id)
SETTINGS index_granularity = 8192;
