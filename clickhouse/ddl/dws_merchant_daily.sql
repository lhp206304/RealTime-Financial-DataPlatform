-- DWS 商户日汇总表：dwd_transaction_offline 按（商户 + 天）聚合
--
-- 引擎：ReplacingMergeTree（聚合结果同一商户同一天唯一，ORDER BY 去重）
-- 分区：toYYYYMM(dt)，按月分区
-- 排序键：(dt, merchant_id) 与 StarRocks 主键一致
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.dws_merchant_daily
(
    dt                        Date          NOT NULL COMMENT '业务日期分区列（聚合粒度：商户+天）',
    merchant_id               String        NOT NULL COMMENT '商户ID',
    txn_count                 Int64         NOT NULL COMMENT '交易总笔数 count(*)',
    customer_count            Int64         NOT NULL COMMENT '去重客户数 countDistinct(customer_id)',
    account_count             Int64         NOT NULL COMMENT '去重账户数 countDistinct(account_id)',
    total_amount              Decimal(18,2) NOT NULL COMMENT '交易总额 sum(amount)',
    avg_amount                Float64            NULL COMMENT '平均单笔金额 avg(amount)（Spark avg 返回 DOUBLE）',
    max_amount                Decimal(18,2) NOT NULL COMMENT '单笔最大金额 max(amount)',
    pay_count                 Int64         NOT NULL COMMENT '支付笔数（transaction_type=''PAYMENT''），无匹配为 0',
    pay_amount                Decimal(18,2)      NULL COMMENT '支付金额，当天无支付为 NULL',
    refund_count              Int64         NOT NULL COMMENT '退款笔数（transaction_type=''REFUND''），无匹配为 0',
    refund_amount             Decimal(18,2)      NULL COMMENT '退款金额，当天无退款为 NULL',
    currency_count            Int64         NOT NULL COMMENT '涉及币种数 countDistinct(currency)',
    cny_amount                Decimal(18,2)      NULL COMMENT 'CNY 交易金额，当天无 CNY 交易为 NULL',
    usd_amount                Decimal(18,2)      NULL COMMENT 'USD 交易金额，当天无 USD 交易为 NULL',
    eur_amount                Decimal(18,2)      NULL COMMENT 'EUR 交易金额，当天无 EUR 交易为 NULL',
    high_risk_txn_count       Int64         NOT NULL COMMENT '高风险商户交易笔数（merchant_risk_level=''HIGH''），无匹配为 0',
    high_risk_customer_count  Int64         NOT NULL COMMENT '在高风险商户交易过的去重客户数，无匹配为 0',
    refund_rate               Float64       NOT NULL COMMENT '退款笔数占比 refund_count/txn_count（保留 4 位小数；txn_count≥1 不会除零）'
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, merchant_id)
SETTINGS index_granularity = 8192;
