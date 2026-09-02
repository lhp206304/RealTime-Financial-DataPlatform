CREATE TABLE IF NOT EXISTS finance.dwd_transaction (
    transaction_id    VARCHAR(64)     NOT NULL  COMMENT "交易ID，主键",
    event_time        DATETIME        NOT NULL  COMMENT "事件时间",
    customer_id       VARCHAR(32)     NOT NULL  COMMENT "用户ID",
    account_id        VARCHAR(32)     NOT NULL  COMMENT "账户ID",
    merchant_id       VARCHAR(32)     NOT NULL  COMMENT "商户ID",
    amount            DECIMAL(18, 2)  NOT NULL  COMMENT "金额",
    currency          VARCHAR(8)      NOT NULL  COMMENT "币种",
    transaction_type  VARCHAR(16)     NOT NULL  COMMENT "交易类型"
)
PRIMARY KEY (transaction_id, event_time, customer_id)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- USE finance;

-- INSERT INTO dwd_transaction VALUES
-- ('T00001', '2026-08-31 10:00:00', 'C12345', 'A11111', 'M22222', 100.50, 'CNY', 'PAYMENT'),
-- ('T00002', '2026-08-31 10:01:00', 'C67890', 'A33333', 'M44444', 88.88, 'USD', 'REFUND');

-- SELECT * FROM dwd_transaction;

-- -- 验证主键去重：再插一条相同主键、金额不同的
-- INSERT INTO dwd_transaction VALUES
-- ('T00001', '2026-08-31 10:00:00', 'C12345', 'A11111', 'M22222', 999.99, 'CNY', 'PAYMENT');

-- SELECT * FROM dwd_transaction WHERE transaction_id = 'T00001';
-- -- 金额应变成 999.99，证明主键覆盖生效