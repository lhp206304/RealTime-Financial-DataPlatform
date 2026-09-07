-- ADS 客户当天画像：dws_customer_daily 当天分区 → 派生指标 + 打标分层
--
-- 引擎：ReplacingMergeTree（画像结果同一客户同一天唯一，ORDER BY 去重）
-- 分区：toYYYYMM(dt)，按月分区
-- 排序键：(dt, customer_id) 与 StarRocks 主键一致
-- BOOLEAN → ClickHouse 用 UInt8（0/1）
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.ads_customer_profile
(
    dt                 Date          NOT NULL COMMENT '快照日期分区列（当天画像）',
    customer_id        String        NOT NULL COMMENT '客户ID',
    txn_count          Int64         NOT NULL COMMENT '当天交易笔数（DWS 透传）',
    total_amount       Decimal(18,2) NOT NULL COMMENT '当天交易总额（DWS 透传）',
    pay_count          Int64         NOT NULL COMMENT '当天支付笔数（DWS 透传）',
    pay_amount         Decimal(18,2)      NULL COMMENT '当天支付金额，当天无支付为 NULL',
    refund_count       Int64         NOT NULL COMMENT '当天退款笔数（DWS 透传）',
    refund_amount      Decimal(18,2)      NULL COMMENT '当天退款金额，当天无退款为 NULL',
    avg_ticket_amount  Float64       NOT NULL COMMENT '当天客单价 total_amount/txn_count',
    refund_txn_rate    Float64       NOT NULL COMMENT '当天退款笔数占比 refund_count/txn_count',
    pay_amount_ratio   Float64       NOT NULL COMMENT '支付金额占比 pay_amount/total_amount（无支付按 0）',
    has_refund         UInt8         NOT NULL COMMENT '当天是否发生过退款（0=否，1=是）',
    amount_tier        String        NOT NULL COMMENT '当天价值分层：HIGH≥1万 / MID≥1千 / LOW',
    ma7_total_amount   Decimal(18,2)      NULL COMMENT '7 日移动平均交易总额',
    ma7_txn_count      Int64              NULL COMMENT '7 日移动平均交易笔数',
    dod_amount_change  Decimal(18,2)      NULL COMMENT '日环比交易总额（昨天）'
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, customer_id)
SETTINGS index_granularity = 8192;
