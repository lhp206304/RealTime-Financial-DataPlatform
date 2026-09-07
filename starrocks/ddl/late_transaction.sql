-- 迟到交易明细表：Flink Job1 在 source 层判定的迟到数据（event_time < CURRENT_WATERMARK）
--
-- 模型选择：UNIQUE KEY（主键模型）
--   迟到数据可能因 checkpoint 恢复重复写入，主键 (transaction_id, event_time) 保证幂等覆盖。
--   Flink sink 表定义 PRIMARY KEY (transaction_id, event_time)，与 StarRocks 主键对齐。
-- 分区：不按天分区（Flink sink 无 dt 列，字段必须严格对齐）。
--   如需清理历史迟到数据，可手动 DELETE WHERE event_time < ...。
-- 分桶：按 customer_id HASH，便于按客户维度排查迟到原因。
CREATE TABLE IF NOT EXISTS finance.late_transaction (
    transaction_id   VARCHAR(64)     NOT NULL  COMMENT "交易ID",
    event_time       DATETIME        NOT NULL  COMMENT "事件时间（毫秒精度）",
    customer_id      VARCHAR(32)     NOT NULL  COMMENT "客户ID",
    account_id       VARCHAR(32)     NOT NULL  COMMENT "账户ID",
    merchant_id      VARCHAR(32)     NOT NULL  COMMENT "商户ID",
    amount           DECIMAL(18, 2)  NOT NULL  COMMENT "金额（DECIMAL 精确，不用浮点）",
    currency         VARCHAR(8)      NOT NULL  COMMENT "币种：CNY/USD/EUR",
    transaction_type VARCHAR(16)     NOT NULL  COMMENT "交易类型：PAYMENT/REFUND"
)
DUPLICATE KEY (transaction_id, event_time)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
