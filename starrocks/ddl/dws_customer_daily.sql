-- DWS 客户日汇总表：dwd_transaction_offline 按（客户 + 天）聚合
--
-- 模型选择：Primary Key（主键模型）
--   DWS 是聚合结果，同一客户同一天只有一行；重跑/修正时按主键 upsert 覆盖，
--   不像明细表那样先 DELETE 再 append。主键 = 粒度列（customer_id, dt）。
--   注意：StarRocks 主键模型要求分区列必须包含在主键里 → 主键写成 (dt, customer_id)。
-- 分区：date_trunc('day', dt) 表达式分区，与 writer 的按分区覆盖（dt 参数）配套。
-- 分桶：按 customer_id HASH，与 DWD 上游一致，客户维度查询裁剪效率高。
--
-- 指标口径（对齐 dws_customer_daily.py 的 aggregate）：
--   条件聚合把 transaction_type 信息压成列（支付/退款各一组笔数+金额），
--   行数不膨胀；sum(when(...)) 无匹配时为 NULL（不是 0），所以金额列允许 NULL。
-- 字段类型对齐 Spark 输出：
--   count → BIGINT；sum(DECIMAL(18,2)) → DECIMAL(18,2)（上游同精度）；
--   avg(DECIMAL) → DOUBLE（Spark avg 的返回类型；金额均值如需精确可改为
--   total_amount / txn_count 再 cast DECIMAL，此处按代码现状用 DOUBLE）。
CREATE TABLE IF NOT EXISTS finance.dws_customer_daily (
    dt            DATE            NOT NULL  COMMENT "业务日期分区列（聚合粒度：客户+天）",
    customer_id   VARCHAR(32)     NOT NULL  COMMENT "客户ID",
    txn_count     BIGINT          NOT NULL  COMMENT "交易总笔数 count(*)",
    total_amount  DECIMAL(18, 2)  NOT NULL  COMMENT "交易总金额 sum(amount)",
    avg_amount    DOUBLE              NULL  COMMENT "平均单笔金额 avg(amount)（Spark avg 返回 DOUBLE）",
    pay_count     BIGINT          NOT NULL  COMMENT "支付笔数 count(when(type='PAYMENT'))，无匹配为 0",
    pay_amount    DECIMAL(18, 2)      NULL  COMMENT "支付金额 sum(when(type='PAYMENT', amount))，当天无支付为 NULL",
    refund_count  BIGINT          NOT NULL  COMMENT "退款笔数 count(when(type='REFUND'))，无匹配为 0",
    refund_amount DECIMAL(18, 2)      NULL  COMMENT "退款金额 sum(when(type='REFUND', amount))，当天无退款为 NULL"
)
PRIMARY KEY (dt, customer_id)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
