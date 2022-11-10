package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

/**
 * @Author lzc
 * @Date 2022/11/8 09:18
 */
public class SQLUtil {
    
    public static String getKafkaSourceDDL(String topic, String groupId, String... format) {
        
        String f = "json";
        if (format.length > 0) {
            f = format[0];
        }
        
        return "with(" +
            "  'connector' = 'kafka', " +
            "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
            "  'properties.group.id' = '" + groupId + "', " +
            "  'topic' = '" + topic + "', " +
            "  'scan.startup.mode' = 'latest-offset', " +
            "  'format' = '" + f + "' " +
            ")";
    }
    
    public static String getKafkaSinkDDL(String topic) {
        return "with(" +
            "  'connector' = 'kafka', " +
            "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
            "  'topic' = '" + topic + "', " +
            "  'format' = 'json'" +
            ")";
    }
    
    public static String getUpsetKafkaDDL(String topic) {
        return "with(" +
            "  'connector' = 'upsert-kafka', " +
            "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
            "  'topic' = '" + topic + "', " +
            "  'key.format' = 'json', " +
            "  'value.format' = 'json' " +
            ")";
    }
}
