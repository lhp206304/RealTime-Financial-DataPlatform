-- ============================================================
-- Kafka → 清洗（过滤 + Watermark + 去重）→ StarRocks
-- 提交方式：docker exec -it jobmanager ./bin/sql-client.sh，逐段贴入
-- ============================================================

-- 并行度：<= topic 分区数，避免空转 subtask
SET 'parallelism.default' = '3';

-- ------------------------------------------------------------
-- ① Kafka Source 表（字段对齐 Transaction 的 JSON）
--    WATERMARK：声明 event_time 为事件时间，容忍 5 秒乱序
-- ------------------------------------------------------------
create table if not exists kafka_source_transaction (
    transaction_id string,
    amount decimal(18,2),
    currency string,
    customer_id string,
    account_id string,
    merchant_id string,
    transaction_type string,
    event_time timestamp_ltz(3),   -- 带 Z（UTC）→ 用 LTZ 接；普通 timestamp(3) 不认 Z
    watermark for event_time as event_time - interval '5' second
)
with(
    'connector' = 'kafka',
    'topic' = 'transaction',
    'properties.bootstrap.servers' = 'broker:19092',
    'properties.group.id' = 'transaction-group',
    'format'='json',
    'json.timestamp-format.standard'='ISO-8601',  -- event_time 是 ISO8601（带 T）；配合 TIMESTAMP_LTZ 接住带 Z 的 UTC 时间
    'json.ignore-parse-errors'='true',  -- 遇到非法 JSON（脏消息）跳过该条，而不是让整个 job 崩
    'scan.startup.mode'='latest-offset'
);

-- ------------------------------------------------------------
-- ② StarRocks Sink 表（字段/类型和 dwd_transaction.sql 完全一致）
-- ------------------------------------------------------------
create table if not exists sink_dwd_transaction (
    transaction_id string,
    event_time timestamp_ltz(3),   -- 和 source 对齐；写入 StarRocks DATETIME 会转成对应时刻
    customer_id string,
    account_id string,
    merchant_id string,
    amount decimal(18,2),
    currency string,
    transaction_type string,
    primary key (transaction_id, event_time, customer_id) not enforced
)
with(
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks:9030', -- 9030：JDBC，用于查询/建表
    -- load-url 直连 BE 8040：allin1 镜像 FE(8030) 会把 Stream Load 307 重定向到 127.0.0.1:8040，
    -- 跨容器时 127.0.0.1 = Flink 自己 → Connection refused。直连 BE 绕过重定向。
    'load-url' = 'starrocks:8040',
    -- 关事务型 Stream Load：事务模式会向 FE 查 BE 列表，拿到的还是 127.0.0.1，同样连不上
    'sink.at-least-once.use-transaction-stream-load' = 'false',
    -- flush 触发：到间隔就写。不配的话小数据量会长时间卡在 buffer 里不落库
    -- max-rows 合法范围 [64000, 5000000]；本地量小，主要靠 interval-ms 定时触发
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.buffer-flush.max-rows' = '64000',
    'database-name' = 'finance',
    'table-name' = 'dwd_transaction',
    'username' = 'root',
    'password' = ''
);

-- ------------------------------------------------------------
-- ③ 清洗 + 去重后写入 Sink
--    基础过滤：金额 > 0、主键字段非空（对齐 Pydantic 校验 + StarRocks NOT NULL）
--    注意：json.ignore-parse-errors 会把脏消息(如 'hello')解析成"全 NULL 行"，
--          不是丢弃。所以必须在这里用 IS NOT NULL 挡掉，否则 NULL 主键写入 StarRocks 会崩 job。
--    去重：每个 transaction_id 按 event_time 取最新一条（rn = 1）
-- ------------------------------------------------------------
insert into sink_dwd_transaction
select
    transaction_id,
    event_time,
    customer_id,
    account_id,
    merchant_id,
    amount,
    currency,
    transaction_type
from (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by event_time desc
        ) as rn
    from kafka_source_transaction
    where amount > 0
      and transaction_id is not null
      and event_time is not null
      and customer_id is not null
)
where rn = 1;
