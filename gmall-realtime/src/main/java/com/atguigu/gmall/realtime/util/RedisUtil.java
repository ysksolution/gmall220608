package com.atguigu.gmall.realtime.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author lzc
 * @Date 2022/11/15 13:53
 */
public class RedisUtil {
    
    private static final JedisPool pool;
    
    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);  // 连接池最多提供 100 个连接
        config.setMaxIdle(10);
        config.setMinIdle(2);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);  // ping pong
        config.setTestOnReturn(true);
        config.setMaxWaitMillis(10 * 1000); // 当连接池没有空闲链接的时候,等待时间
        
        pool = new JedisPool(config, "hadoop162", 6379);
    }
    public static Jedis getRedisClient() {
        // redis-server /etc/redis.conf
        Jedis jedis = pool.getResource();
        jedis.select(4);  // 选择 4 号库
        return jedis;
    }
}
