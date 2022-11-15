package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/11/15 09:00
 */
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
            throw new RuntimeException("每有查到对应的维度信息, 请检测你的维度表和 id 的值是否正确: 表名=" + table + ",id=" + id);
        }
    
        return dims.get(0); // 查到的维度信息返回
        
    }
    
}
