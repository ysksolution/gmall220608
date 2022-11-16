package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/11/16 11:42
 */
public class Dws_10_DwsTradeProvinceOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_10_DwsTradeProvinceOrderWindow().init(
            4010,
            2,
            "Dws_10_DwsTradeProvinceOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
    
        SingleOutputStreamOperator<TradeProvinceOrderBean> streamWithoutDim = stream
            .map(new MapFunction<String, TradeProvinceOrderBean>() {
                @Override
                public TradeProvinceOrderBean map(String value) throws Exception {
                
                
                    JSONObject obj = JSON.parseObject(value);
                    return TradeProvinceOrderBean.builder()
                        .orderDetailId(obj.getString("id"))
                        .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                        .provinceId(obj.getString("province_id"))
                        .orderAmount(obj.getBigDecimal("split_total_amount"))
                        .ts(obj.getLong("ts") * 1000)
                        .build();
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeProvinceOrderBean::getOrderDetailId)
            .process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {
            
                private ValueState<TradeProvinceOrderBean> beanState;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    beanState = getRuntimeContext().getState(new ValueStateDescriptor<TradeProvinceOrderBean>("state", TradeProvinceOrderBean.class));
                }
            
                @Override
                public void processElement(TradeProvinceOrderBean bean,
                                           Context ctx,
                                           Collector<TradeProvinceOrderBean> out) throws Exception {
                    TradeProvinceOrderBean lastBean = beanState.value();
                
                    if (lastBean == null) {
                        beanState.update(bean);
                    
                        out.collect(bean);
                    } else {
                        // 由于对象是可变对象, 所以最好 copy 一个新的对象, 再存入到状态中
                        TradeProvinceOrderBean newBean = new TradeProvinceOrderBean();
                        BeanUtils.copyProperties(newBean, bean); // 把右边的对象的属性 copy 到左边的对象中
                        beanState.update(newBean);// 更新前的数据
                    
                        bean.setOrderAmount(bean.getOrderAmount().subtract(lastBean.getOrderAmount()));
                        out.collect(bean);
                    }
                
                }
            })
            .keyBy(TradeProvinceOrderBean::getProvinceId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1,
                                                         TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String provinceId,
                                        Context ctx,
                                        Iterable<TradeProvinceOrderBean> elements,
                                        Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean bean = elements.iterator().next();
                        bean.setStt(AtguiguUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(ctx.window().getEnd()));
                    
                        bean.setOrderCount((long) bean.getOrderIdSet().size());
                    
                        bean.setTs(System.currentTimeMillis());
                    
                        out.collect(bean);
                    
                    }
                }
            );
    
        SingleOutputStreamOperator<TradeProvinceOrderBean> result = AsyncDataStream.unorderedWait(
            streamWithoutDim,
            new DimAsyncFunction<TradeProvinceOrderBean>() {
                @Override
                protected String getTable() {
                    return "dim_base_province";
                }
            
                @Override
                protected String getId(TradeProvinceOrderBean input) {
                    return input.getProvinceId();
                }
            
                @Override
                protected void addDim(TradeProvinceOrderBean input, JSONObject dim) {
                    input.setProvinceName(dim.getString("NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        result.addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_province_order_window", TradeProvinceOrderBean.class));
    
    
    }
}
/*
省略粒度 下单统计
1. 数据源: 下单事务事实表

2. 去重: 按照订单详情id

3. keyBy: 按照省份 id   开窗聚合
    order_amount
    
    order_count
    
        详情id    订单id   set 集合 下单数
        1          1       set(1)  1    0-5
        2          1       set(1)
        3          2       set(2)
        3          3       set(3)
            .....
            
            
                set(1,2,3) -> 订单数: 3
                -----
      
        
    添加一个字段: set 集合, 存储订单 id
    
4. 写出到clickhouse 中

 */