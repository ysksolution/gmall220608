package com.atguigu.gmall.realtime.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author lzc
 * @Date 2022/11/4 11:50
 */
public class FlinkSourceUtil {
    
    // 返回一个 kafka source: FlinkKafkaConsumer
    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        return null;
    }
}
