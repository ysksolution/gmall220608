package com.atguigu.gmall.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

/**
 * @Author lzc
 * @Date 2022/11/5 11:35
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    
    private Connection conn;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DruidDSUtil.getPhoenixConn();
    }
    
    @Override
    public void close() throws Exception {
        if (conn != null) {
            // 1. 如果链接对象是直接获取, 则是关闭连接
            // 2. 如果连接是从连接池获取的, 则是归还连接
            conn.close();
            
        }
    }
    
    //流中每来一条数据,则执行一次这个方法
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> tp, Context context) throws Exception {
        // 实现写入业务
        // jdbc: 执行一个插入语句
    }
}
/*
长连接问题:
    mysql 当一个连接超过 8 小时与服务器没有通讯, 则服务器会自动关闭连接.

解决长链接问题;
    1. 每隔一段时间, 与服务器做一次通讯
        select 1;
    2. 使用前, 先判断这个链接是否还在链接, 如果没有, 重新获取一个新的链接
    
    3. 使用连接池
        连接池会避免连接关闭
        
        druid 德鲁伊
 */
