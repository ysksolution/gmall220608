package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
    
        stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                    .withIdleness(Duration.ofSeconds(20))
            )
            .keyBy(obj -> obj.getString("user_id"))
            .flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            
                private ValueState<String> lastCartAddDateState;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDateState", String.class));
                }
            
                @Override
                public void flatMap(JSONObject value,
                                    Collector<CartAddUuBean> out) throws Exception {
                    long ts = value.getLong("ts") * 1000; // ts是 maxwell 生成的, 是秒
                
                    String today = AtguiguUtil.tsToDate(ts);
                    String lastCartAddDate = lastCartAddDateState.value();
                    Long cartAddUuCt = 0L;
                    if (!today.equals(lastCartAddDate)) {
                        cartAddUuCt = 1L;
                        lastCartAddDateState.update(today);
                    
                    }
                    if (cartAddUuCt == 1) {
                        out.collect(new CartAddUuBean("", "", cartAddUuCt, ts));
                    }
                
                }
            })
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1,
                                                CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void process(Context context,
                                        Iterable<CartAddUuBean> elements,
                                        Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean bean = elements.iterator().next();
                        bean.setStt(AtguiguUtil.tsToDateTime(context.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(context.window().getEnd()));
                    
                        bean.setTs(System.currentTimeMillis());
                    
                        out.collect(bean);
                    }
                }
            )
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_cart_add_uu_window", CartAddUuBean.class));
        
        
    }
}
