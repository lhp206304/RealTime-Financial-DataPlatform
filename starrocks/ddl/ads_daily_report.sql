-- ADS 每日大盘报表：dws_merchant_daily 上卷到（天）粒度 + 派生比率 + 同环比 + 月累计
--
-- 模型选择：Primary Key，主键 = (dt)，一天一行。
-- 分区：date_trunc('day', dt)；分桶 HASH(dt) BUCKETS 1（一天一行，数据量极小）。
--
-- 指标口径（对齐 ads_daily_report.py 的 build）：
--   上卷规则：计数/金额 sum、极值 max、均值重算（total/txn）；去重类指标
--   （customer_count 等）不可跨粒度上卷，大盘层不算（需 UV 时回 DWD 重算）。
--   同环比：lag(refund_rate) 取昨日，历史第一天无昨日 → prev/diff 为 NULL。
--   月累计：按自然月 rolling sum，依赖「当天及以前」全量数据（重跑历史天不影响）。
-- 字段类型对齐 Spark 输出：
--   sum(count) → BIGINT；sum(DECIMAL) → DECIMAL(18,2)；比率/客单价（除法）→ DOUBLE。
CREATE TABLE IF NOT EXISTS finance.ads_daily_report (
    dt                     DATE            NOT NULL  COMMENT "业务日期分区列（粒度：天，一天一行）",
    month                  VARCHAR(7)      NOT NULL  COMMENT "自然月 yyyy-MM（月累计分组键）",
    active_merchant_count  BIGINT          NOT NULL  COMMENT "当天活跃商户数（dws 行数）",
    total_txn_count        BIGINT          NOT NULL  COMMENT "全平台交易笔数 sum(txn_count)",
    total_amount           DECIMAL(18, 2)  NOT NULL  COMMENT "全平台交易总额 sum(total_amount)",
    pay_count              BIGINT          NOT NULL  COMMENT "支付笔数 sum(pay_count)",
    pay_amount             DECIMAL(18, 2)      NULL  COMMENT "支付金额 sum(pay_amount)，当天无支付为 NULL",
    refund_count           BIGINT          NOT NULL  COMMENT "退款笔数 sum(refund_count)",
    refund_amount          DECIMAL(18, 2)      NULL  COMMENT "退款金额 sum(refund_amount)，当天无退款为 NULL",
    cny_amount             DECIMAL(18, 2)      NULL  COMMENT "CNY 交易总额",
    usd_amount             DECIMAL(18, 2)      NULL  COMMENT "USD 交易总额",
    eur_amount             DECIMAL(18, 2)      NULL  COMMENT "EUR 交易总额",
    high_risk_txn_count    BIGINT          NOT NULL  COMMENT "高风险商户交易笔数 sum(high_risk_txn_count)",
    max_amount             DECIMAL(18, 2)  NOT NULL  COMMENT "全平台单笔最大金额 max(max_amount)",
    avg_ticket_amount      DOUBLE              NULL  COMMENT "客单价 total_amount/total_txn_count",
    refund_rate            DOUBLE          NOT NULL  COMMENT "退款率 refund_count/total_txn_count",
    high_risk_txn_ratio    DOUBLE          NOT NULL  COMMENT "高风险交易占比 high_risk_txn_count/total_txn_count",
    prev_refund_rate       DOUBLE              NULL  COMMENT "昨日退款率（lag），历史第一天为 NULL",
    refund_rate_diff       DOUBLE              NULL  COMMENT "退款率环比变化（今-昨），正=恶化；历史第一天为 NULL",
    month_total_txn_count  BIGINT          NOT NULL  COMMENT "月初至今累计交易笔数",
    month_total_amount     DECIMAL(18, 2)  NOT NULL  COMMENT "月初至今累计交易额"
)
PRIMARY KEY (dt)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
