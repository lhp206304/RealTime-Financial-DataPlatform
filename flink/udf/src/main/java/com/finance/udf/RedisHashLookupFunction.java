package com.finance.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import redis.clients.jedis.JedisPooled;

import java.util.Map;

/**
 * 通用 Redis Hash 维表查询 UDTF。
 * 传入完整 Redis key，返回该 key 下所有字段的 MAP，SQL 层自取所需字段。
 * 一个函数通吃所有 Redis Hash 维度表，无需为每张表单独写函数。
 */
@FunctionHint(output = @DataTypeHint("ROW<data MAP<STRING, STRING>>"))
public class RedisHashLookupFunction extends TableFunction<Row> {

    private transient JedisPooled jedis;

    @Override
    public void open(FunctionContext context) {
        jedis = new JedisPooled("redis", 6379);
    }

    /**
     * @param key 完整的 Redis key，例如 "dim:customer:C001"
     *            调用方负责拼接：keyPrefix || ':' || id
     */
    public void eval(String key) {
        if (key == null) {
            collect(Row.of(Map.of()));
            return;
        }
        Map<String, String> data = hgetall(key);
        collect(Row.of(data));
    }

    private Map<String, String> hgetall(String key) {
        try {
            return jedis.hgetAll(key);
        } catch (Exception e) {
            return Map.of();   // Redis 异常：返回空 Map，SQL 层 [] 取值得 null，不影响流
        }
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}