package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Author lzc
 * @Date 2022/11/4 11:33
 */
@Slf4j
public class DimApp extends BaseAppV1 {
    public static void main(String[] args) {
        // 每个子类要消费的 topic 肯定是不一样的
        new DimApp().init(
            2001,
            2,
            "DimApp",
            Constant.TOPIC_ODS_DB
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 这里完成你业务逻辑
        
        // 1. 对流中的做数据清洗 etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        // 2. 读取配置表的数据
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        // 3. 在 phoenix 中建表
        tpStream = createTableOnPhoenix(tpStream);
        
        // 4. 数据流和配置做 connect
        connectStreams(etledStream, tpStream);
        
        // 2. 现在数据既有事实表又维度表, 我们只要维度表的数据: 过滤出需要的所有维度表数据
        // 使用动态的方式过滤出想要的维度
        
        // 3. 把不同的数据写出到 phoenix 中的不同的表中
        
    }
    
    private void connectStreams(SingleOutputStreamOperator<JSONObject> dataStream,
                                SingleOutputStreamOperator<TableProcess> tpStream) {
        // 广播 key: user_info:ALL
        // 广播 value: TableProcess
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        // 1. 把配置流做成广播流
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(tpStateDesc);
        // 2. 数据流去 connect 广播流
        dataStream
            .connect(bcStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
    
                // 4. 当数据信息来的时候, 从广播状态读取配置信息
                @Override
                public void processElement(JSONObject obj,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    String key = obj.getString("table") + ":ALL";
                    TableProcess tp = state.get(key);
                    if (tp != null) {
                        out.collect(Tuple2.of(obj, tp));
                    }
                }
    
                // 3. 把配置信息放入到广播状态
                @Override
                public void processBroadcastElement(TableProcess tp,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    String key = tp.getSourceTable() + ":" + tp.getSourceType();
                    state.put(key, tp);
                }
            })
            .print();
        
        
        
        
        
        
    }
    
    private SingleOutputStreamOperator<TableProcess> createTableOnPhoenix(
        SingleOutputStreamOperator<TableProcess> tpStream) {
        return tpStream
            .filter(tp -> "dim".equals(tp.getSinkType()))
            .map(new RichMapFunction<TableProcess, TableProcess>() {
                private Connection conn;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 1. 建立到 phoenix 的连接
                    conn = JdbcUtil.getPhoenixConnection();
                }
                
                @Override
                public void close() throws Exception {
                    if (conn != null) {
                        conn.close();
                    }
                }
                
                @Override
                public TableProcess map(TableProcess tp) throws Exception {
                    // 根据每条配置信息,在 Phoenix 中建表
                    // create table if not exists user(id varchar, age varchar, sex varchar, constraint pk primary key(id)) SALT_BUCKETS = 4;
                    
                    String op = tp.getOp();
                    StringBuilder sql = null;
                    if ("r".equals(op) || "c".equals(op)) {
                        sql = getCreateTableSQL(tp);
                    } else if ("d".equals(op)) {
                        sql = getDelTableSQL(tp);
                    } else {
                        // 先删
                        StringBuilder delSQL = getDelTableSQL(tp);
                        PreparedStatement ps = conn.prepareStatement(delSQL.toString());
                        ps.execute();
                        ps.close();
                        //再建
                        sql = getCreateTableSQL(tp);
                    }
                    
                    log.warn(sql.toString());
                    // 2. 获取一个预处理语句
                    PreparedStatement ps = conn.prepareStatement(sql.toString());
                    // 3. 执行建表语句
                    ps.execute();
                    // 4. 关闭资源(预处理语句和连接对象)
                    ps.close();
                    return tp;
                }
                
                private StringBuilder getDelTableSQL(TableProcess tp) {
                    return new StringBuilder().append("drop table if exists ").append(tp.getSinkTable());
                }
                
                private StringBuilder getCreateTableSQL(TableProcess tp) {
                    StringBuilder sql = new StringBuilder();
                    sql
                        .append("create table if not exists ")
                        .append(tp.getSinkTable())
                        .append("(")
                        .append(tp.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                        .append(", constraint pk primary key(")
                        .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())
                        .append("))")
                        .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
                    return sql;
                }
            });
    }
    
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop162")
            .port(3306)
            .databaseList("gmall_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
            .tableList("gmall_config.table_process") // set captured table
            .username("root")
            .password("aaaaaa")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();
        return env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc")
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                String op = obj.getString("op");
                String beforeOrAfter = "after";
                if ("d".equals(op)) {
                    beforeOrAfter = "before";
                }
                TableProcess tp = obj.getObject(beforeOrAfter, TableProcess.class);
                tp.setOp(op);
                return tp;
            });
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    /*
                    1. 都应该是 json 格式, 格式不对是脏数据
                    2. 库是 gmall2022
                     */
                    try {
                        JSONObject obj = JSON.parseObject(value.replaceAll("bootstrap-", ""));
                        
                        String type = obj.getString("type");
                        String data = obj.getString("data");
                        
                        return "gmall2022".equals(obj.getString("database"))
                            && obj.getString("table") != null
                            && ("insert".equals(type) || "update".equals(type))
                            && data != null
                            && data.length() > 2;
                        
                    } catch (Exception e) {
                        System.out.println("数据格式有误, 不是 json 数据: " + value);
                        return false;
                    }
                }
            })
            .map(json -> JSON.parseObject(json.replaceAll("bootstrap-", "")));
    }
}
/*
把 需要哪些报错一个配置信息中, flink 能够实时的读取配置信息的变化, 当配置信息变化之后
flink 程序可以不用做任何的变动, 实时对配置的变化进程处理

广播状态:

把配置信息做成一个广播流, 与数据流进行 connect, 把配置信息放入广播状态, 数据信息读取广播状态

找一个位置存储配置信息:
    mysql 中
    
----
cdc:
   op:
      r  启动的时候, 读取的快照  before=null  after 有
      u  更新字段 before 有 after 有
      d  删除字段  before 有 after=null
      c  创建数据  before=null  after 有
      
      更新的主键:
      先 d 后 c
      
---------
 SALT_BUCKETS = 4
    创建盐表
    
 region server
 
 region: 每张表会有一个或多个 region
 
 region 分裂:
    旧版本
        当 region 的大小增长到 10G 的时候, 会一分为 2
    新版本
        min(10g, 2 * n^3 * 128 M)
         2 * 8 * 128
         
 region 迁移:
    尽量让同一张表的多个 region 分布在不同的 region server
    
 生成环境, 为了避免 region 的分裂和迁移, 使用预分区表(一个分区就是一个 region)
 
 
 -----------
 phoenix 也要建预分区表: 盐表
         
         

 */













