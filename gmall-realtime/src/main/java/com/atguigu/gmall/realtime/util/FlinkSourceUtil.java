package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/11/4 11:50
 */
public class FlinkSourceUtil {
    
    // 返回一个 kafka source: FlinkKafkaConsumer
    public static SourceFunction<String> getKafkaSource(String groupId,
                                                        String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.setProperty("group.id", groupId);
        // 消费数据的时候, 应该只消费已提交的数据
        props.setProperty("isolation.level", "read_committed");  // read_uncommitted
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>(
            topic,
            new KafkaDeserializationSchema<String>() {
                
                // 返回流中数据的类型信息
                @Override
                public TypeInformation<String> getProducedType() {
                    // 内置的类型
                    //                    return Types.STRING;
                    // 类型不包含泛型
                    //                    return TypeInformation.of(String.class);
                    // 类型又包含泛型
                    return TypeInformation.of(new TypeHint<String>() {});
                }
                
                // 要不要停止流
                // kafka 读数据, 应该是无界流, 永远返回 false
                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }
                
                // 反序列化
                @Override
                public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                    if (record.value() != null) {
                        return new String(record.value(), StandardCharsets.UTF_8);
                    }
                    return null;
                }
            },
            props
        
        );
        // 从最新的 offset 消费:   如果没有消费过, 则从最新的开始消费, 如果checkpoint 中有消费记录, 则从上次的位置继续消费
        source.setStartFromLatest();
        return source;
    }
}
