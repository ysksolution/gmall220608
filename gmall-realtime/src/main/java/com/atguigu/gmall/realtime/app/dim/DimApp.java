package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connectStreams(etledStream, tpStream);
        // 5. 去掉不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = delNoNeedColumns(connectedStream);
        // 6. 把不同的数据写出到 phoenix 中的不同的表中
        writeToPhoenix(resultStream);
        
    }
    
    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream) {
        /*
        没有专门的 phoenix sink, 所以需要自定义
        
        1. 能不能使用 jdbc sink
            如果使用 jdbc sink, 流中所有数据只能写入到一个 表中.
            
            因为我们流中有多张表的数据, 所以不能使用 jdbc sink
            
        2. 自定义 sink
         */
        resultStream.addSink(FlinkSinkUtil.getPhoenixSink());
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delNoNeedColumns(
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        return stream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> tp) throws Exception {
                JSONObject data = tp.f0;
                // data中的 key 在 columns 中存在就保留不存在就删除
                List<String> columns = Arrays.asList(tp.f1.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !columns.contains(key) && !"op_type".equals(key));
                return tp;
            }
        });
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStreams(
        SingleOutputStreamOperator<JSONObject> dataStream,
        SingleOutputStreamOperator<TableProcess> tpStream) {
        // 广播 key: user_info:ALL
        // 广播 value: TableProcess
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        // 1. 把配置流做成广播流
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(tpStateDesc);
        // 2. 数据流去 connect 广播流
        return dataStream
            .connect(bcStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                
                private HashMap<String, TableProcess> tpMap;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 先把所有的配置全部加载进来
                    // 预加载配置信息
                    // 放入什么地方?
                    // 不能放入状态: 因为在 open 状态不能使用
                    // 放入 HashMap 中
                    // 获取配置信息的时候, 先从状态中获取, 状态中没有再从 HashMap 中获取
                    preLoadTableProcess();
                    
                }
                
                // 对配置进行预加载
                private void preLoadTableProcess() {
                    tpMap = new HashMap<>();
                    Connection conn = JdbcUtil.getMySqlConnection();
                    // 查询一个表中所有的行的数据
                    List<TableProcess> tpList = JdbcUtil.queryList(conn,
                                                                   "select * from gmall_config.table_process",
                                                                   TableProcess.class,
                                                                   true);
                    
                    // 把每行数据放入到 map 中
                    for (TableProcess tp : tpList) {
                        String key = tp.getSourceTable() + ":" + tp.getSourceType();
                        tpMap.put(key, tp);
                    }
                    
                    // 日志中打预加载
                    String msg = "\n配置信息预加载: \n\t\t";
                    for (Map.Entry<String, TableProcess> kv : tpMap.entrySet()) {
                        String key = kv.getKey();
                        TableProcess v = kv.getValue();
                        msg += key + "=>" + v + "\n\t\t";
                    }
                    log.warn(msg);
                    
                }
                
                // 4. 当数据信息来的时候, 从广播状态读取配置信息
                // 如果数据流来的比较早, 配置信息来的比较晚, 则会导致部分数据丢失
                @Override
                public void processElement(JSONObject obj,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    String key = obj.getString("table") + ":ALL";
                    TableProcess tp = state.get(key);
                    if (tp == null) { // 广播状态没有获取对应的配置信息, 去 hashMap 中获取
                        tp = tpMap.get(key);
                    }
                    
                    if (tp != null) {
                        // 把 type 值放入到 data 中, 到后期要用
                        JSONObject data = obj.getJSONObject("data");
                        data.put("op_type", obj.getString("type"));
                        out.collect(Tuple2.of(data, tp));
                        
                    }
                }
                
                // 3. 把配置信息放入到广播状态
                @Override
                public void processBroadcastElement(TableProcess tp,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    String key = tp.getSourceTable() + ":" + tp.getSourceType();
                    if ("d".equals(tp.getOp())) {
                        state.remove(key); // 如果配置信息删除. 删除状态中的数据
                        tpMap.remove(key);  // 把预加载的配置信息也需要删除
                    } else {
                        
                        state.put(key, tp);
                    }
                    
                    
                    // 考虑配置信息的更新和删除
                    // 0. 新增的时候, 直接状态会新增数据(不用额外操作)
                    // 1. 当更新的时候, 状态中新数据会覆盖旧数据(不用额外操作)
                    // 2. 如果配置信息删除. 删除状态中的数据
                    
                }
            });
        
        
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
                    if (conn.isClosed()) {
                        conn = JdbcUtil.getPhoenixConnection();
                    }
                    
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













