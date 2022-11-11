package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/11/11 08:29
 */
public class Dwd_08_BaseDBApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_08_BaseDBApp().init(
            3008,
            2,
            "Dwd_08_BaseDBApp",
            Constant.TOPIC_ODS_DB
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        // 2. 通过flink cdc 读配置表数据
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        // 3. 数据流和配置进行 connect
        connect(etledStream, tpStream);
        
        // 4. 过滤掉不需要的字段
        
        // 5. 不同表的数据写出到不同的 topic 种
    }
    
    private void connect(SingleOutputStreamOperator<JSONObject> dataStream,
                         SingleOutputStreamOperator<TableProcess> tpStream) {
        //1. 配置流做成广播流
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(tpStateDesc);
        dataStream
            .connect(bcStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                @Override
                public void processElement(JSONObject obj,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 根据 key 来获取对应的配置信息
                    JSONObject data = obj.getJSONObject("data");
                    JSONObject old = obj.getJSONObject("old");
    
                    String table = obj.getString("table");
                    String type = obj.getString("type");
                    String key =  table+ ":" + type;
                    
                    if("coupon_use".equals(table) && "update".equals(type)){
                        String newStatus = data.getString("coupon_status");
                        String oldStatus = old.getString("coupon_status");
                        if ("1401".equals(oldStatus) && "1402".equals(newStatus)) {
                            key += "{\"data\": {\"coupon_status\": \"1402\"}, \"old\": {\"coupon_status\": \"1401\"}}";
                        }else if("1402".equals(oldStatus) && "1403".equals(newStatus)){
                            key += "{\"data\": {\"used_time\": \"not null\"}}";
                        }
                    }
                    
                    ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                    TableProcess tp = tpState.get(key);
    
                    if (tp != null) {
                        out.collect(Tuple2.of(data,tp));
                    }
                    
    
                }
                
                @Override
                public void processBroadcastElement(TableProcess tp,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    String key = tp.getSourceTable()
                        + ":" + tp.getSourceType()
                        + (tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
    
                    BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    state.put(key, tp);
                    
                }
            })
            .print();
        
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
            })
            // 只过滤出事实表的配置信息
            .filter(tp -> "dwd".equals(tp.getSinkType()));
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
工具域:
    优惠券的领取
        coupon_use insert
    优惠券使用(下单)
        coupon_use update && old['coupon_status'] ='1401' && data['coupon_status'] ='1402'
    优惠券使用(支付)
        coupon_use update && old['coupon_status'] ='1402' && data['coupon_status'] ='1403'
        
交互域:
    common_info insert
    favor_info insert
    
用户域:
    用户注册事务事实表
    user_info insert
 */