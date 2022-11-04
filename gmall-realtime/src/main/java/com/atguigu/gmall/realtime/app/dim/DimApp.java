package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/4 11:33
 */
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
        SingleOutputStreamOperator<String> etledStream = etl(stream);
        etledStream.print();
    
        // 2. 现在数据既有事实表又维度表, 我们只要维度表的数据: 过滤出需要的所有维度表数据
        // 使用动态的方式过滤出想要的维度
        
        // 3. 把不同的数据写出到 phoenix 中的不同的表中
        
    }
    
    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> stream) {
       return stream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    /*
                    1. 都应该是 json 格式, 格式不对是脏数据
                    2. 库是 gmall2022
                     */
                    try {
                        JSONObject obj = JSON.parseObject(value);
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
            });
    }
}
/*
把 需要哪些报错一个配置信息中, flink 能够实时的读取配置信息的变化, 当配置信息变化之后
flink 程序可以不用做任何的变动, 实时对配置的变化进程处理

广播状态:

把配置信息做成一个广播流, 与数据流进行 connect, 把配置信息放入广播状态, 数据信息读取广播状态

找一个位置存储配置信息:
    mysql 中

 */













