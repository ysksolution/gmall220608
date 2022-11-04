package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/11/4 11:50
 */
public class FlinkSourceUtil {
    
    // 返回一个 kafka source: FlinkKafkaConsumer
    public static SourceFunction<String> getKafkaSource(String groupId,
                                                        String topic) {
        Properties props = new Properties() ;
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.setProperty("group.id", groupId);
        // 消费数据的时候, 应该只消费已提交的数据
        props.setProperty("isolation.level", "read_committed");  // read_uncommitted
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>(
            topic,
            new SimpleStringSchema(),
            props
    
        );
        // 从最新的 offset 消费:   如果没有消费过, 则从最新的开始消费, 如果checkpoint 中有消费记录, 则从上次的位置继续消费
        source.setStartFromLatest();
        return source;
    }
}
