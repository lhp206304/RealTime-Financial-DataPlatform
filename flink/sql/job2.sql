-- ============================================================
-- Job2：Kafka DWD topic → Watermark + 迟到处理 + 窗口聚合 → StarRocks
-- 提交方式：
-- docker exec -i jobmanager ./bin/sql-client.sh -f /opt/flink/sql/job2.sql
-- 注意：每个 SQL 语句后要空行，否则会报错
-- ============================================================

-- ---- 作业级配置 ----
SET 'parallelism.default' = '3';
SET 'pipeline.name' = 'Job2-窗口聚合-DWS';
SET 'table.exec.source.idle-timeout' = '30s';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'AT_LEAST_ONCE';
SET 'execution.checkpointing.timeout' = '60s';

-- ------------------------------------------------------------
-- ① Kafka Source 表（订阅 Job1 产出的 DWD topic）
--    WATERMARK：声明 event_time 为事件时间，容忍 5 秒乱序
-- ------------------------------------------------------------
create table if not exists kafka_source_dwd_transaction (
    transaction_id string,
    event_time timestamp_ltz(3),
    customer_id string, account_id string, merchant_id string,
    amount decimal(18,2), currency string, transaction_type string,
    customer_level string, customer_region string, customer_register_time string,
    merchant_category string, merchant_risk_level string,
    watermark for event_time as event_time - interval '5' second
   )
with(
    'connector' = 'kafka',
    'topic' = 'dwd_transaction',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'dwd_transaction-group',
    'format'='json',
    'json.timestamp-format.standard'='ISO-8601',  -- event_time 是 ISO8601（带 T）；配合 TIMESTAMP_LTZ 接住带 Z 的 UTC 时间
    'json.ignore-parse-errors'='true',  -- 遇到非法 JSON（脏消息）跳过该条，而不是让整个 job 崩
    'scan.startup.mode'='latest-offset'
);

-- ------------------------------------------------------------
-- ② StarRocks Sink 表（dws_realtime_agg）
--    window_type 区分 TUMBLE / HOP 两种窗口的结果
-- ------------------------------------------------------------
create table if not exists sink_dws_realtime_agg (
    window_type          string,
    window_start         timestamp_ltz(3),
    window_end           timestamp_ltz(3),
    customer_id          string,
    total_amount         decimal(18,2),
    avg_amount           double,
    max_amount           decimal(18,2),
    min_amount           decimal(18,2),
    transaction_count    bigint,
    merchant_count       bigint,
    account_count        bigint,
    currency_count       bigint,
    txn_type_count       bigint,
    high_risk_txn_count  bigint,
    large_txn_amount     decimal(18,2),
    first_txn_time       timestamp_ltz(3),
    last_txn_time        timestamp_ltz(3),
    primary key (window_type, window_start, window_end, customer_id) not enforced
)
with(
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks:9030',
    'load-url' = 'starrocks:8040',
    'sink.at-least-once.use-transaction-stream-load' = 'false',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.buffer-flush.max-rows' = '64000',
    'database-name' = 'finance',
    'table-name' = 'dws_realtime_agg',
    'username' = 'root',
    'password' = ''
);

-- ------------------------------------------------------------
-- ③ 窗口聚合 INSERT（TUMBLE + HOP 并成一个作业）
--    window_type 用字面量 'TUMBLE' / 'HOP' 区分两种窗口
-- ------------------------------------------------------------

EXECUTE STATEMENT SET
BEGIN
    -- 滚动窗口：每 1 小时统计一次，窗口不重叠
    INSERT INTO sink_dws_realtime_agg
    SELECT
        'TUMBLE' as window_type,
        window_start,
        window_end,
        customer_id,
        -- ---- 金额指标 ----
        sum(amount) as total_amount,                          -- 总金额
        avg(amount) as avg_amount,                            -- 平均金额
        max(amount) as max_amount,                            -- 最大单笔金额
        min(amount) as min_amount,                            -- 最小单笔金额
        -- ---- 笔数指标 ----
        count(distinct transaction_id) as transaction_count,  -- 交易笔数（去重）
        count(distinct merchant_id) as merchant_count,       -- 活跃商户数
        count(distinct account_id) as account_count,           -- 活跃账户数
        count(distinct currency) as currency_count,           -- 涉及币种数
        count(distinct transaction_type) as txn_type_count,  -- 交易类型数
        -- ---- 风控指标 ----
        count(case when merchant_risk_level = 'HIGH' then 1 end) as high_risk_txn_count,  -- 高风险商户交易数
        sum(case when amount > 10000 then amount else 0 end) as large_txn_amount,         -- 大额交易金额（>1万）
        -- ---- 时间指标 ----
        min(event_time) as first_txn_time,                   -- 窗口内首笔交易时间
        max(event_time) as last_txn_time                     -- 窗口内末笔交易时间
    FROM TABLE (
        TUMBLE (
            TABLE kafka_source_dwd_transaction,
            DESCRIPTOR(event_time),
            INTERVAL '5' minute
        )
    )
    WHERE amount > 0
      AND customer_id IS NOT NULL
    GROUP BY
        window_start,
        window_end,
        customer_id;

    -- 滑动窗口：每 5 分钟滑一次，统计近 10 分钟的交易情况（窗口有重叠）
    INSERT INTO sink_dws_realtime_agg
    SELECT
        'HOP' as window_type,
        window_start,
        window_end,
        customer_id,
        -- ---- 金额指标 ----
        sum(amount) as total_amount,                          -- 总金额
        avg(amount) as avg_amount,                            -- 平均金额
        max(amount) as max_amount,                            -- 最大单笔金额
        min(amount) as min_amount,                            -- 最小单笔金额
        -- ---- 笔数指标 ----
        count(distinct transaction_id) as transaction_count,  -- 交易笔数（去重）
        count(distinct merchant_id) as merchant_count,       -- 活跃商户数
        count(distinct account_id) as account_count,           -- 活跃账户数
        count(distinct currency) as currency_count,           -- 涉及币种数
        count(distinct transaction_type) as txn_type_count,  -- 交易类型数
        -- ---- 风控指标 ----
        count(case when merchant_risk_level = 'HIGH' then 1 end) as high_risk_txn_count,  -- 高风险商户交易数
        sum(case when amount > 10000 then amount else 0 end) as large_txn_amount,         -- 大额交易金额（>1万）
        -- ---- 时间指标 ----
        min(event_time) as first_txn_time,                   -- 窗口内首笔交易时间
        max(event_time) as last_txn_time                     -- 窗口内末笔交易时间
    FROM TABLE (
        HOP (
            TABLE kafka_source_dwd_transaction,
            DESCRIPTOR(event_time),
            INTERVAL '5' minute,   -- 滑动步长：每 5 分钟出一次结果
            INTERVAL '10' minute    -- 窗口大小：统计近 10 分钟的数据
        )
    )
    WHERE amount > 0
      AND customer_id IS NOT NULL
    GROUP BY
        window_start,
        window_end,
        customer_id;
END;
