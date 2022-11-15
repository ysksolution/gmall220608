package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.Constant;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/11/15 09:00
 */
@Slf4j
public class DimUtil {
    public static void main(String[] args) {
    
    }
    
    public static JSONObject readDimFromPhoenix(Connection conn,
                                                String table,
                                                String id) {
        String sql = "select * from " + table + " where id=?";
    
        List<JSONObject> dims = JdbcUtil.queryList(conn,
                                                   sql,
                                                   new String[]{id}, // 数组的长度是几就表示占位符的个数
                                                   JSONObject.class);
        if (dims.size() == 0) {
            // 表示没有查到相应的维度信息
            throw new RuntimeException("没有查到对应的维度信息, 请检测你的维度表名和 id 的值是否正确: 表名=" + table + ",id=" + id);
        }
        return dims.get(0); // 查到的维度信息返回
    }
    
    public static JSONObject readDim(Jedis redisClient,
                                     Connection phoenixConn,
                                     String table,
                                     String id) {
        // 1. 先从 redis 读取配置信息
        JSONObject dim = readDimFromRedis(redisClient, table, id);
        // 2. 如果读到了, 则直接返回
        if (dim == null) {
            log.warn("走数据:" + table + " " + id);
            dim = readDimFromPhoenix(phoenixConn, table,id);
        // 3. 如果没有读到, 则从 phoenix 读取, 然后再写入到缓存
            writeDimToRedis(redisClient, table, id, dim);
        }else{
            log.warn("走缓存:" + table + " " + id);
        }
        return dim;
    }
    
 
    private static void writeDimToRedis(Jedis redisClient, String table, String id, JSONObject dim) {
        // key:  table:id
        String key = getRedisKey(table, id);
        /*redisClient.set(key, dim.toJSONString());
        // 设置 ttl
        redisClient.expire(key, 2 * 24 * 60 * 60);*/
        redisClient.setex(key, Constant.TTL_TWO_DAYS, dim.toJSONString());
        
    }
    
   
    // TODO
    private static JSONObject readDimFromRedis(Jedis redisClient, String table, String id) {
        String key = getRedisKey(table, id);
        String json = redisClient.get(key);  // 读取一个字符串
        JSONObject dim = null;
        if (json != null) {
            dim = JSON.parseObject(json);  // 把字符串解析成一个 JSONObject 对象
        }
        return dim;
    }
    
    private static String getRedisKey(String table, String id) {
        return table + ":" + id;
    }
    
}
