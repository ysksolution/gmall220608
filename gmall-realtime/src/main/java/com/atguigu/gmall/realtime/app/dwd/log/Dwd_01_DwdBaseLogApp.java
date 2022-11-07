package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/7 09:21
 */
@Slf4j
public class Dwd_01_DwdBaseLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_01_DwdBaseLogApp().init(
            3001,
            2,
            "Dwd_01_DwdBaseLogApp",
            Constant.TOPIC_ODS_LOG
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        //        1. 读取 ods_log 数据
        //        2. 对数据做 etl
        etl(stream);
        //        3. 纠正新老客户
        //        4. 分流
        //        5. 不同的流的数据写入到不同的 topic 种
    }
    
    private void etl(DataStreamSource<String> stream) {
        stream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    try {
                        JSON.parseObject(value);
                    } catch (Exception e) {
                        log.warn("日志格式不对, 不是合法的 json 日志: " + value);
                        return false;
                    }
                    return true;
                }
            })
            .print();
    }
}
