package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/12 11:43
 */
public class Dws_06_DwsTradeCartAddUuWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_06_DwsTradeCartAddUuWindow().init(
            4005,
            2,
            "Dws_06_DwsTradeCartAddUuWindow",
            Constant.TOPIC_DWD_TRADE_CART_ADD
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        
    }
}
