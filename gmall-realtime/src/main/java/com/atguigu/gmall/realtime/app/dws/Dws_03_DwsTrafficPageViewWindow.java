package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
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
 * @Date 2022/11/12 10:26
 */
public class Dws_03_DwsTrafficPageViewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_03_DwsTrafficPageViewWindow().init(
            4003,
            2,
            "Dws_03_DwsTrafficPageViewWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
    
        stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .filter(obj -> {
                String pageId = obj.getJSONObject("page").getString("page_id");
                return "home".equals(pageId) || "good_detail".equals(pageId);
            })
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
                private ValueState<String> visitHomeDateState;
                private ValueState<String> visitGoodDetailDateState;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    visitHomeDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitHomeDateState", String.class));
                    visitGoodDetailDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitGoodDetailDateState", String.class));
                }
            
                @Override
                public void flatMap(JSONObject obj, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    Long ts = obj.getLong("ts");
                    String pageId = obj.getJSONObject("page").getString("page_id");
                    String today = AtguiguUtil.tsToDate(ts);
                    Long homeUvCt = 0L;
                    if ("home".equals(pageId) && !today.equals(visitHomeDateState.value())) {
                        homeUvCt = 1L;
                        visitHomeDateState.update(today);
                    }
                    Long goodDetailUvCt = 0L;
                    if ("good_detail".equals(pageId) && !today.equals(visitGoodDetailDateState.value())) {
                        goodDetailUvCt = 1L;
                        visitGoodDetailDateState.update(today);
                    }
                
                    TrafficHomeDetailPageViewBean bean = new TrafficHomeDetailPageViewBean(
                        "", "",
                        homeUvCt, goodDetailUvCt,
                        ts
                    );
                
                    if (homeUvCt + goodDetailUvCt == 1) {
                        out.collect(bean);
                    }
                
                }
            })
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1,
                                                                TrafficHomeDetailPageViewBean bean2) throws Exception {
                        bean1.setHomeUvCt(bean1.getHomeUvCt() + bean2.getHomeUvCt());
                        bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt() + bean2.getGoodDetailUvCt());
                        return bean1;
                    }
                },
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<TrafficHomeDetailPageViewBean> elements,
                                        Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                    
                        bean.setStt(AtguiguUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(ctx.window().getEnd()));
                    
                        bean.setTs(System.currentTimeMillis());
                    
                        out.collect(bean);
                    }
                }
            )
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_traffic_page_view_window", TrafficHomeDetailPageViewBean.class));
        
    }
}
/*
 统计当日的首页和商品详情页独立访客数。
 
 1. 只过滤首页和详情页数据
 2. 解析成 pojo 类型
 3. 开窗聚合
 4. 写出到 clickhouse 中
 
*/