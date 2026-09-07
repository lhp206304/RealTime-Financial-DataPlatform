-- 实时 DWS 聚合表：Flink Job2 窗口聚合结果（TUMBLE + HOP）
--
-- 模型选择：PRIMARY KEY（主键模型）
--   窗口聚合结果按主键 upsert：同一窗口同一客户重算时覆盖，不产生重复行。
--   主键 = (window_type, window_start, window_end, customer_id)，window_type 区分两种窗口。
-- 分区：不分区（实时数据量小，查询以最近窗口为主）。
--   如需清理历史窗口，可手动 DELETE WHERE window_end < ...。
-- 分桶：按 customer_id HASH，与离线 DWS 一致，便于按客户维度查询。
--
-- 字段类型对齐 Flink SQL 输出：
--   Flink TIMESTAMP_LTZ → StarRocks DATETIME
--   Flink DECIMAL(18,2) → StarRocks DECIMAL(18,2)
--   Flink avg() → DOUBLE（Flink avg 返回 DOUBLE，不是 DECIMAL）
--   Flink count() → BIGINT
CREATE TABLE IF NOT EXISTS finance.dws_realtime_agg (
    window_type          VARCHAR(16)     NOT NULL  COMMENT "窗口类型：TUMBLE（滚动）/ HOP（滑动）",
    window_start         DATETIME        NOT NULL  COMMENT "窗口开始时间",
    window_end           DATETIME        NOT NULL  COMMENT "窗口结束时间",
    customer_id          VARCHAR(32)     NOT NULL  COMMENT "客户ID",
    total_amount         DECIMAL(18, 2)  NOT NULL  COMMENT "总金额 sum(amount)",
    avg_amount           DOUBLE              NULL  COMMENT "平均金额 avg(amount)（Flink avg 返回 DOUBLE）",
    max_amount           DECIMAL(18, 2)      NULL  COMMENT "最大单笔金额",
    min_amount           DECIMAL(18, 2)      NULL  COMMENT "最小单笔金额",
    transaction_count    BIGINT          NOT NULL  COMMENT "交易笔数（去重 transaction_id）",
    merchant_count       BIGINT          NOT NULL  COMMENT "活跃商户数（去重 merchant_id）",
    account_count        BIGINT          NOT NULL  COMMENT "活跃账户数（去重 account_id）",
    currency_count       BIGINT          NOT NULL  COMMENT "涉及币种数",
    txn_type_count       BIGINT          NOT NULL  COMMENT "交易类型数",
    high_risk_txn_count  BIGINT          NOT NULL  COMMENT "高风险商户交易数",
    large_txn_amount     DECIMAL(18, 2)  NOT NULL  COMMENT "大额交易金额（>1万）",
    first_txn_time       DATETIME            NULL  COMMENT "窗口内首笔交易时间",
    last_txn_time        DATETIME            NULL  COMMENT "窗口内末笔交易时间"
)
PRIMARY KEY (window_type, window_start, window_end, customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
