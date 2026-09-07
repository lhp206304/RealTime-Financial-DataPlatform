-- ADS 客户当天画像：dws_customer_daily 当天分区 → 派生指标 + 打标分层，按天分区快照
--
-- 模型选择：Primary Key，主键 = (dt, customer_id)；每天 dt 分区存当天画像。
-- 分区：date_trunc('day', dt)；分桶 HASH(customer_id) BUCKETS 4。
--
-- 粒度说明：DWS 当天分区一行 = 一个客户，本层不做再聚合（无 groupBy），
--   只做「透传 + 派生比率 + 打标分层」，全部指标都是当天口径（非累计）。
-- 派生口径（对齐 ads_customer_profile.py 的 build）：
--   客单价/占比 = 聚合结果列之间重算；pay_amount 当天无支付为 NULL → coalesce 0。
--   amount_tier 分档阈值：HIGH ≥ 1万、MID ≥ 1千、其余 LOW（业务可调）。
CREATE TABLE IF NOT EXISTS finance.ads_customer_profile (
    dt                DATE            NOT NULL  COMMENT "快照日期分区列（当天画像）",
    customer_id       VARCHAR(32)     NOT NULL  COMMENT "客户ID",
    txn_count         BIGINT          NOT NULL  COMMENT "当天交易笔数（DWS 透传）",
    total_amount      DECIMAL(18, 2)  NOT NULL  COMMENT "当天交易总额（DWS 透传）",
    pay_count         BIGINT          NOT NULL  COMMENT "当天支付笔数（DWS 透传）",
    pay_amount        DECIMAL(18, 2)      NULL  COMMENT "当天支付金额，当天无支付为 NULL",
    refund_count      BIGINT          NOT NULL  COMMENT "当天退款笔数（DWS 透传）",
    refund_amount     DECIMAL(18, 2)      NULL  COMMENT "当天退款金额，当天无退款为 NULL",
    avg_ticket_amount DOUBLE          NOT NULL  COMMENT "当天客单价 total_amount/txn_count",
    refund_txn_rate   DOUBLE          NOT NULL  COMMENT "当天退款笔数占比 refund_count/txn_count",
    pay_amount_ratio  DOUBLE          NOT NULL  COMMENT "支付金额占比 pay_amount/total_amount（无支付按 0）",
    has_refund        BOOLEAN         NOT NULL  COMMENT "当天是否发生过退款",
    amount_tier       VARCHAR(8)      NOT NULL  COMMENT "当天价值分层：HIGH≥1万 / MID≥1千 / LOW"
)
PRIMARY KEY (dt, customer_id)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
