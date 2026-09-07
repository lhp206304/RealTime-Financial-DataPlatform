-- ADS 商户销售额 Top10 榜单：dws_merchant_daily 按天窗口排名取前 10 + 当日销售额占比
--
-- 引擎：ReplacingMergeTree（一天 10 行，ORDER BY 去重）
-- 分区：toYYYYMM(dt)，按月分区
-- 排序键：(dt, rank_no) 与 StarRocks 主键一致（名次每天重排，按天覆盖重跑幂等）
-- 指标口径（对齐 ads_merchant_top10.py 的 build）：
--   rank_no：row_number() over (partition by dt order by total_amount desc, txn_count desc)，
--   销售额并列时按笔数定序，名次稳定可复现；占比分母 = 当天全部商户销售额之和。
CREATE DATABASE IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.ads_merchant_top10_daily
(
    dt              Date          NOT NULL COMMENT '业务日期分区列（粒度：天+名次）',
    rank_no         Int32         NOT NULL COMMENT '当日销售额名次 1-10（row_number）',
    merchant_id     String        NOT NULL COMMENT '商户ID',
    txn_count       Int64         NOT NULL COMMENT '该商户当天交易笔数',
    customer_count  Int64         NOT NULL COMMENT '该商户当天去重客户数',
    total_amount    Decimal(18,2) NOT NULL COMMENT '该商户当天交易总额',
    amount_share    Float64       NOT NULL COMMENT '销售额占比 total_amount/当日全平台总额'
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, rank_no)
SETTINGS index_granularity = 8192;
