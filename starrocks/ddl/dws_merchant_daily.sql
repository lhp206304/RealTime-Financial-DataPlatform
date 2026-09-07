-- DWS 商户日汇总表：dwd_transaction_offline 按（商户 + 天）聚合
--
-- 模型选择：Primary Key（主键模型）
--   聚合结果按主键 upsert 覆盖（重跑/修正友好）；分区列必须进主键 → (dt, merchant_id)。
-- 分区：date_trunc('day', dt) 表达式分区，与 writer 按分区覆盖（dt 参数）配套。
-- 分桶：按 merchant_id HASH，与 DWD 上游一致，商户维度查询裁剪效率高。
--
-- 指标口径（对齐 dws_merchant_daily.py 的 aggregate，17 个聚合指标 + 1 个派生比率）：
--   规模：笔数/去重客户数/去重账户数/总额/均额/单笔最大
--   交易类型：PAYMENT、REFUND 条件聚合（count(when) 无匹配得 0 → NOT NULL；
--            sum(when) 无匹配得 NULL → 金额列允许 NULL）
--   币种：CNY/USD/EUR 拆列（混币种 sum 无意义），无该币种交易为 NULL
--   风险：merchant_risk_level='HIGH' 的笔数与去重客户数
--         （countDistinct 忽略 NULL，全 NULL 时返回 0，故 NOT NULL）
-- 字段类型对齐 Spark 输出：
--   count/countDistinct → BIGINT；sum(DECIMAL(18,2)) → DECIMAL(18,2)（上游同精度）；
--   avg(DECIMAL)/round(x/y,4) → DOUBLE。
CREATE TABLE IF NOT EXISTS finance.dws_merchant_daily (
    dt                        DATE            NOT NULL  COMMENT "业务日期分区列（聚合粒度：商户+天）",
    merchant_id               VARCHAR(32)     NOT NULL  COMMENT "商户ID",
    txn_count                 BIGINT          NOT NULL  COMMENT "交易总笔数 count(*)",
    customer_count            BIGINT          NOT NULL  COMMENT "去重客户数 countDistinct(customer_id)",
    account_count             BIGINT          NOT NULL  COMMENT "去重账户数 countDistinct(account_id)",
    total_amount              DECIMAL(18, 2)  NOT NULL  COMMENT "交易总额 sum(amount)",
    avg_amount                DOUBLE              NULL  COMMENT "平均单笔金额 avg(amount)（Spark avg 返回 DOUBLE）",
    max_amount                DECIMAL(18, 2)  NOT NULL  COMMENT "单笔最大金额 max(amount)",
    pay_count                 BIGINT          NOT NULL  COMMENT "支付笔数（transaction_type='PAYMENT'），无匹配为 0",
    pay_amount                DECIMAL(18, 2)      NULL  COMMENT "支付金额，当天无支付为 NULL",
    refund_count              BIGINT          NOT NULL  COMMENT "退款笔数（transaction_type='REFUND'），无匹配为 0",
    refund_amount             DECIMAL(18, 2)      NULL  COMMENT "退款金额，当天无退款为 NULL",
    currency_count            BIGINT          NOT NULL  COMMENT "涉及币种数 countDistinct(currency)",
    cny_amount                DECIMAL(18, 2)      NULL  COMMENT "CNY 交易金额，当天无 CNY 交易为 NULL",
    usd_amount                DECIMAL(18, 2)      NULL  COMMENT "USD 交易金额，当天无 USD 交易为 NULL",
    eur_amount                DECIMAL(18, 2)      NULL  COMMENT "EUR 交易金额，当天无 EUR 交易为 NULL",
    high_risk_txn_count       BIGINT          NOT NULL  COMMENT "高风险商户交易笔数（merchant_risk_level='HIGH'），无匹配为 0",
    high_risk_customer_count  BIGINT          NOT NULL  COMMENT "在高风险商户交易过的去重客户数，无匹配为 0",
    refund_rate               DOUBLE          NOT NULL  COMMENT "退款笔数占比 refund_count/txn_count（保留 4 位小数；txn_count≥1 不会除零）"
)
PRIMARY KEY (dt, merchant_id)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(merchant_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
