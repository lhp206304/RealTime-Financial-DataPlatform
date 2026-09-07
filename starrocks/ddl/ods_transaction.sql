-- ODS 交易明细表：MinIO fact 桶原始交易原样落地（只做类型对齐 + 派生 dt 分区列）
--
-- 模型选择：Duplicate Key（明细模型）
--   ODS 不做去重/聚合，幂等由写入侧保证（writer 先 DELETE WHERE dt=当天 再 append），
--   明细模型 append 最快，也没有主键模型"分区列/分桶列必须进主键"的约束。
-- 分区：date_trunc('day', dt) 表达式分区，数据写入时按 dt 自动建分区，无需手动维护。
-- 分桶：按 customer_id HASH，客户维度查询/后续 JOIN 打宽时裁剪效率高。
CREATE TABLE IF NOT EXISTS finance.ods_transaction (
    transaction_id    VARCHAR(64)     NOT NULL  COMMENT "交易ID",
    event_time        DATETIME        NOT NULL  COMMENT "事件时间（毫秒精度）",
    amount            DECIMAL(18, 2)  NOT NULL  COMMENT "金额（DECIMAL 精确，不用浮点）",
    currency          VARCHAR(8)      NOT NULL  COMMENT "币种，如 CNY",
    customer_id       VARCHAR(32)     NOT NULL  COMMENT "客户ID",
    account_id        VARCHAR(32)     NOT NULL  COMMENT "账户ID",
    merchant_id       VARCHAR(32)     NOT NULL  COMMENT "商户ID",
    transaction_type  VARCHAR(16)     NOT NULL  COMMENT "交易类型：PAYMENT/REFUND 等",
    dt                DATE            NOT NULL  COMMENT "业务日期分区列 = to_date(event_time)，全链路统一按 dt 分区"
)
DUPLICATE KEY (transaction_id, event_time)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
