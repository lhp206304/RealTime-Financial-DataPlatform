-- DWS 客户日汇总表：dwd_transaction_offline 按（客户 + 天）聚合
--
-- 引擎：ReplacingMergeTree（聚合结果同一客户同一天唯一，ORDER BY 去重）
-- 分区：toYYYYMM(dt)，按月分区
-- 排序键：(dt, customer_id) 与 StarRocks 主键一致，支持按天/客户查询
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.dws_customer_daily
(
    dt            Date          NOT NULL COMMENT '业务日期分区列（聚合粒度：客户+天）',
    customer_id   String        NOT NULL COMMENT '客户ID',
    txn_count     Int64         NOT NULL COMMENT '交易总笔数 count(*)',
    total_amount  Decimal(18,2) NOT NULL COMMENT '交易总金额 sum(amount)',
    avg_amount    Float64            NULL COMMENT '平均单笔金额 avg(amount)（Spark avg 返回 DOUBLE）',
    pay_count     Int64         NOT NULL COMMENT '支付笔数 count(when(type=''PAYMENT''))，无匹配为 0',
    pay_amount    Decimal(18,2)      NULL COMMENT '支付金额 sum(when(type=''PAYMENT'', amount))，当天无支付为 NULL',
    refund_count  Int64         NOT NULL COMMENT '退款笔数 count(when(type=''REFUND''))，无匹配为 0',
    refund_amount Decimal(18,2)      NULL COMMENT '退款金额 sum(when(type=''REFUND'', amount))，当天无退款为 NULL'
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, customer_id)
SETTINGS index_granularity = 8192;
