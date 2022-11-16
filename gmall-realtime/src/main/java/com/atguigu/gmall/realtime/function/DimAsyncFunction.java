package com.atguigu.gmall.realtime.function;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.RedisUtil;
import com.atguigu.gmall.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author lzc
 * @Date 2022/11/16 09:23
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    
    private ThreadPoolExecutor threadPool;
    
    protected abstract String getTable();
    protected abstract String getId(T input);
    protected abstract void addDim(T input, JSONObject dim);
    
    
    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();
        
    }
    
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) {
        // 多线程+多客户端
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                
                try {
                    // 获取 redis 客户端 和 phoenix 客户端
                    Connection phoenixConn = DruidDSUtil.getPhoenixConn();
                    Jedis redisClient = RedisUtil.getRedisClient();
                    // 使用 DimUtil 读取维度数据
                    JSONObject dim = DimUtil.readDim(redisClient, phoenixConn, getTable(), getId(input));
                    // 关闭连接
                    phoenixConn.close();
                    redisClient.close();
                    
                    // 补充到input 中
                    addDim(input, dim);
                    // 把 input 放入到ResultFuture中
                    resultFuture.complete(Collections.singletonList(input));
                    
                    
                    
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
    
                
                
                
                
                
            }
        });
        
    }
}
