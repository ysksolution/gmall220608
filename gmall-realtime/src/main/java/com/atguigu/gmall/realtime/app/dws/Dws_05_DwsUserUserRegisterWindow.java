package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
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
public class Dws_05_DwsUserUserRegisterWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_05_DwsUserUserRegisterWindow().init(
            4005,
            2,
            "Dws_05_DwsUserUserRegisterWindow",
            Constant.TOPIC_DWD_USER_REGISTER
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        stream
            .map(str -> {
                JSONObject obj = JSON.parseObject(str);
            
                return new UserRegisterBean("", "", 1L, obj.getLong("create_time"));
            
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
                    .withIdleness(Duration.ofSeconds(20))
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1,
                                                   UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                
                    @Override
                    public void process(Context ctx,
                                        Iterable<UserRegisterBean> elements,
                                        Collector<UserRegisterBean> out) throws Exception {
                    
                        UserRegisterBean bean = elements.iterator().next();
                        bean.setStt(AtguiguUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(ctx.window().getEnd()));
                    
                        bean.setTs(System.currentTimeMillis());
                    
                        out.collect(bean);
                    
                    }
                }
            )
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_user_user_register_window", UserRegisterBean.class));
        
    }
}
