-- ADS 商户销售额 Top10 榜单：dws_merchant_daily 按天窗口排名取前 10 + 当日销售额占比
--
-- 模型选择：Primary Key，主键 = (dt, rank_no)，一天 10 行。
--   注意：榜单名次每天都会变，按天分区覆盖重跑（writer 先清当天分区）天然幂等。
-- 分区：date_trunc('day', dt)；分桶 HASH(merchant_id) BUCKETS 3。
--
-- 指标口径（对齐 ads_merchant_top10.py 的 build）：
--   rank_no：row_number() over (partition by dt order by total_amount desc, txn_count desc)，
--   销售额并列时按笔数定序，名次稳定可复现；占比分母 = 当天全部商户销售额之和。
CREATE TABLE IF NOT EXISTS finance.ads_merchant_top10_daily (
    dt               DATE            NOT NULL  COMMENT "业务日期分区列（粒度：天+名次）",
    rank_no          INT             NOT NULL  COMMENT "当日销售额名次 1-10（row_number）",
    merchant_id      VARCHAR(32)     NOT NULL  COMMENT "商户ID",
    txn_count        BIGINT          NOT NULL  COMMENT "该商户当天交易笔数",
    customer_count   BIGINT          NOT NULL  COMMENT "该商户当天去重客户数",
    total_amount     DECIMAL(18, 2)  NOT NULL  COMMENT "该商户当天交易总额",
    amount_share     DOUBLE          NOT NULL  COMMENT "销售额占比 total_amount/当日全平台总额"
)
PRIMARY KEY (dt, rank_no)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(merchant_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
