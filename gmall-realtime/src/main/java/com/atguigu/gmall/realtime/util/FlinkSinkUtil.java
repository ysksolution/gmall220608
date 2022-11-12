package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @Author lzc
 * @Date 2022/11/5 11:34
 */
public class FlinkSinkUtil {
    
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }
    
    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        
        // transaction.max.timeout.ms 服务器
        // transaction.timeout.ms 生产者
        props.put("transaction.timeout.ms", 15 * 60 * 1000);
        
        return new FlinkKafkaProducer<String>(
            "default",
            new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                @Nullable Long timestamp) {
                    return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
    
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        
        // transaction.max.timeout.ms 服务器
        // transaction.timeout.ms 生产者
        props.put("transaction.timeout.ms", 15 * 60 * 1000);
        
        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
            "default",
            new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> t,
                                                                @Nullable Long timestamp) {
                    String data = t.f0.toJSONString();
                    String topic = t.f1.getSinkTable();
                    
                    return new ProducerRecord<>(topic, data.getBytes(StandardCharsets.UTF_8));
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        
    }
    
    // 用 jdbc 的方式向 clickhouse 写入数据
    public static <T> SinkFunction<T> getClickHouseSink(String table,
                                                        Class<T> tClass) {
        String drier = Constant.CLICKHOUSE_DRIVER;
        String url = Constant.CLICKHOUSE_URL;
        // insert into person(id,age, name)values(?,?,?)
        String columns = Arrays
            .stream(tClass.getDeclaredFields())
            .map(f -> CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, f.getName()))
            .collect(Collectors.joining(","));
        
        StringBuilder sql = new StringBuilder();
        sql
            .append("insert into ")
            .append(table)
            .append("(")
            //拼接字段名 pojo 的属性名要和表的字段名保持一致
            .append(columns)
            .append(")values(")
            .append(columns.replaceAll("[^,]+", "?"))
            .append(")");
        System.out.println("clickhouse建表语句: " + sql);
        return getJdbcSink(drier, url, sql.toString(), "default", "aaaaaa"); // 如果数据库没有设置用户名和密码,传入2个 null
    }
    
    private static <T> SinkFunction<T> getJdbcSink(String driver,
                                                   String url,
                                                   String sql,
                                                   String user,
                                                   String password) {
        
        return JdbcSink
            .sink(sql,
                  new JdbcStatementBuilder<T>() {
                      @Override
                      public void accept(PreparedStatement ps,
                                         T t) throws SQLException {
                          // insert into a(stt,edt,keyword,keyword_count,ts)values(?,?,?,?,?)
                          // 只做一件事: 给 sql 中的占位符赋值
                          // 要根据你的 sql 语句来进行赋值  TODO
                          // 获取一个类: 类名.class  Class.forName("类名") 对象.getClass()  ClassLoader.getSystemClassLoader().loadClass()
                          Class<?> tClass = t.getClass();
                          Field[] fields = tClass.getDeclaredFields();
                          try {
                              for (int i = 0; i < fields.length; i++) {
                                  Field field = fields[i];
                                  field.setAccessible(true);
                                  Object v = field.get(t);
                                  ps.setObject(i + 1, v);
                              }
                          } catch (IllegalAccessException e) {
                              throw new RuntimeException(e);
                          }
                    
                    
                      }
                  },
                  new JdbcExecutionOptions.Builder()
                      .withBatchSize(1024) // 批次大小
                      .withBatchIntervalMs(3000)  // 每 3 秒刷新一次
                      .withMaxRetries(3)
                      .build(),
                  new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                      .withDriverName(driver)
                      .withUrl(url)
                      .withUsername(user)
                      .withPassword(password)
                      .build()
            );
    }
    
}
