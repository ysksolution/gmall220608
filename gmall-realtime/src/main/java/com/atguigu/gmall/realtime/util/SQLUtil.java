package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

/**
 * @Author lzc
 * @Date 2022/11/8 09:18
 */
public class SQLUtil {
    
    public static String getKafkaSourceDDL(String topic, String groupId) {
        return "with(" +
            "  'connector' = 'kafka', " +
            "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
            "  'properties.group.id' = '" + groupId + "', " +
            "  'topic' = '" + topic + "', " +
            "  'scan.startup.mode' = 'latest-offset', " +
            "  'format' = 'csv'" +
            ")";
    }
}
