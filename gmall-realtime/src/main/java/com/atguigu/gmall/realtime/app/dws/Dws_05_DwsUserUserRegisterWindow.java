package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/12 11:43
 */
public class Dws_05_DwsUserUserRegisterWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_05_DwsUserUserRegisterWindow().init(
            4005,
            2,
            "Dws_05_DwsUserUserRegisterWindow",
            ""
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        
    }
}
