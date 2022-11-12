package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TradePaymentWindowBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/11/12 11:43
 */
public class Dws_07_DwsTradePaymentSucWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_07_DwsTradePaymentSucWindow().init(
            4007,
            2,
            "Dws_07_DwsTradePaymentSucWindow",
            Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
            )
            .keyBy(obj -> obj.getString("user_id"))
            .process(new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {
            
                private ValueState<String> dateState;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    dateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("dateState", String.class));
                }
            
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<TradePaymentWindowBean> out) throws Exception {
                
                    long ts = value.getLong("ts") * 1000;
                    String today = AtguiguUtil.tsToDate(ts);
                    String date = dateState.value();
                    Long paymentSucUniqueUserCount = 0L;
                    Long paymentSucNewUserCount = 0L;
                    if (!today.equals(date)) {
                        paymentSucUniqueUserCount = 1L;
                        dateState.update(today);
                        if (date == null) {
                            paymentSucNewUserCount = 1L;
                        }
                    }
                    if (paymentSucUniqueUserCount == 1) {
                        out.collect(new TradePaymentWindowBean("", "",
                                                               paymentSucUniqueUserCount, paymentSucNewUserCount,
                                                               ts
                        ));
                    }
                }
            })
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1,
                                                         TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context,
                                        Iterable<TradePaymentWindowBean> elements,
                                        Collector<TradePaymentWindowBean> out) throws Exception {
                        TradePaymentWindowBean bean = elements.iterator().next();
                        bean.setStt(AtguiguUtil.tsToDateTime(context.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(context.window().getEnd()));
                    
                        bean.setTs(System.currentTimeMillis());
                    
                        out.collect(bean);
                    }
                }
            )
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_payment_suc_window", TradePaymentWindowBean.class));
        
        
    }
}
