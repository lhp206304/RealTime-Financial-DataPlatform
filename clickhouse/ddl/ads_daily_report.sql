-- ADS 每日大盘报表：dws_merchant_daily 上卷到（天）粒度 + 派生比率 + 同环比 + 月累计
--
-- 引擎：ReplacingMergeTree（一天一行，ORDER BY 去重）
-- 分区：toYYYYMM(dt)，按月分区
-- 排序键：dt（一天一行，主键 = 粒度列）
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.ads_daily_report
(
    dt                     Date          NOT NULL COMMENT '业务日期分区列（粒度：天，一天一行）',
    month                  String        NOT NULL COMMENT '自然月 yyyy-MM（月累计分组键）',
    active_merchant_count  Int64         NOT NULL COMMENT '当天活跃商户数（dws 行数）',
    total_txn_count        Int64         NOT NULL COMMENT '全平台交易笔数 sum(txn_count)',
    total_amount           Decimal(18,2) NOT NULL COMMENT '全平台交易总额 sum(total_amount)',
    pay_count              Int64         NOT NULL COMMENT '支付笔数 sum(pay_count)',
    pay_amount             Decimal(18,2)      NULL COMMENT '支付金额 sum(pay_amount)，当天无支付为 NULL',
    refund_count           Int64         NOT NULL COMMENT '退款笔数 sum(refund_count)',
    refund_amount          Decimal(18,2)      NULL COMMENT '退款金额 sum(refund_amount)，当天无退款为 NULL',
    cny_amount             Decimal(18,2)      NULL COMMENT 'CNY 交易总额',
    usd_amount             Decimal(18,2)      NULL COMMENT 'USD 交易总额',
    eur_amount             Decimal(18,2)      NULL COMMENT 'EUR 交易总额',
    high_risk_txn_count    Int64         NOT NULL COMMENT '高风险商户交易笔数 sum(high_risk_txn_count)',
    max_amount             Decimal(18,2) NOT NULL COMMENT '全平台单笔最大金额 max(max_amount)',
    avg_ticket_amount      Float64            NULL COMMENT '客单价 total_amount/total_txn_count',
    refund_rate            Float64       NOT NULL COMMENT '退款率 refund_count/total_txn_count',
    high_risk_txn_ratio    Float64       NOT NULL COMMENT '高风险交易占比 high_risk_txn_count/total_txn_count',
    prev_refund_rate       Float64            NULL COMMENT '昨日退款率（lag），历史第一天为 NULL',
    refund_rate_diff       Float64            NULL COMMENT '退款率环比变化（今-昨），正=恶化；历史第一天为 NULL',
    month_total_txn_count  Int64         NOT NULL COMMENT '月初至今累计交易笔数',
    month_total_amount     Decimal(18,2) NOT NULL COMMENT '月初至今累计交易额'
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY dt
SETTINGS index_granularity = 8192;
