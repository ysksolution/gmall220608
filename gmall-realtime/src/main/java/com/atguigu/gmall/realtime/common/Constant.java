package com.atguigu.gmall.realtime.common;

/**
 * @Author lzc
 * @Date 2022/11/4 13:55
 */
public class Constant {
    
    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";
    
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop162:3306?useSSL=false";
}
