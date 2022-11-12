package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/12 11:43
 */
public class Dws_07_DwsTradePaymentSucWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_07_DwsTradePaymentSucWindow().init(
            4005,
            2,
            "Dws_07_DwsTradePaymentSucWindow",
            Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        
    }
}
