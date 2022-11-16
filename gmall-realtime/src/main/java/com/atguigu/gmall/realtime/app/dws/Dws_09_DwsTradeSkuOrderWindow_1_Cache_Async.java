package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/11/13 09:13
 */
public class Dws_09_DwsTradeSkuOrderWindow_1_Cache_Async extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_1_Cache_Async().init(
            4009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 封装数据到 pojo 中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);
        // 2. 按照 order_detail_id 去重
        SingleOutputStreamOperator<TradeSkuOrderBean> distinctedStream = distinctByOrderDetailId(beanStream);
        // 3.  开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim = windowAndJoin(distinctedStream);
        // 4. 补充维度信息
        joinDim(beanStreamWithoutDim);
        
    }
    
    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(
            stream,
            new DimAsyncFunction<TradeSkuOrderBean>(),
            60,
            TimeUnit.SECONDS
        );
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndJoin(
        SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeSkuOrderBean::getSkuId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1,
                                                    TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                        value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<TradeSkuOrderBean> elements,
                                        Collector<TradeSkuOrderBean> out) throws Exception {
                        
                        TradeSkuOrderBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(ctx.window().getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream.map(new MapFunction<String, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(String json) throws Exception {
                
                JSONObject obj = JSON.parseObject(json);
                return TradeSkuOrderBean.builder()
                    .orderDetailId(obj.getString("id"))
                    .skuId(obj.getString("sku_id"))
                    .originalAmount(obj.getBigDecimal("split_original_amount"))
                    .orderAmount(obj.getBigDecimal("split_total_amount"))
                    .activityAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal(0) : obj.getBigDecimal("split_activity_amount"))
                    .couponAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal(0) : obj.getBigDecimal("split_coupon_amount"))
                    .ts(obj.getLong("ts") * 1000)
                    .build();
            }
        });
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> distinctByOrderDetailId(
        SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        /*
            order_detail_id  sku_id   分摊总金额   活动表  优惠券
            1                   1       100       null   null
            null
            1                   1       100       有值    null
            1                   1       100       有值    有值
            
            
            只保留最后一个, 因为最后一个的信息最完整!
                 keyBy:order_detail_id
            
                 如果保留第一个, 非常容易做到!
                 
                 保留最后一个?
                    重复的值有 3 种情况: 不重复 重复 2 个 重复 3 个
                    
                 
                 实现去重: 整体指导思想就是用 正负 抵消
                    第一条数据来:
                        数据放入到下游, 进入到窗口中
                            100 null null -> 100 null null
                                把这个条数据存入到状态中
                    第二条数据来:
                            100 有值  null -> a(取出状态中的值): -100 null null
                                              b: 100 有值 null
                                把这个条数据存入到状态中
                                
                    第三条数据来:
                            100 有值  null -> a(取出状态中的值): -100 有值 null
                                              b: 100 有值 有值
       
         */
        return stream
            .keyBy(TradeSkuOrderBean::getOrderDetailId)
            .process(new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {
                
                private ValueState<TradeSkuOrderBean> beanState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    beanState = getRuntimeContext()
                        .getState(new ValueStateDescriptor<TradeSkuOrderBean>("beanState", TradeSkuOrderBean.class));
                }
                
                @Override
                public void processElement(TradeSkuOrderBean bean,
                                           Context ctx,
                                           Collector<TradeSkuOrderBean> out) throws Exception {
                    // 解决数据膨胀:
                    // a   -> a
                    // b   -> b-a
                    // c   -> c-b
                    TradeSkuOrderBean lastBean = beanState.value();
                    
                    if (lastBean == null) {
                        beanState.update(bean);
                        
                        out.collect(bean);
                    } else {
                        // 由于对象是可变对象, 所以最好 copy 一个新的对象, 再存入到状态中
                        TradeSkuOrderBean newBean = new TradeSkuOrderBean();
                        BeanUtils.copyProperties(newBean, bean); // 把右边的对象的属性 copy 到左边的对象中
                        beanState.update(newBean);// 更新前的数据
                        
                        bean.setOriginalAmount(bean.getOriginalAmount().subtract(lastBean.getOriginalAmount()));
                        bean.setOrderAmount(bean.getOrderAmount().subtract(lastBean.getOrderAmount()));
                        bean.setActivityAmount(bean.getActivityAmount().subtract(lastBean.getActivityAmount()));
                        bean.setCouponAmount(bean.getCouponAmount().subtract(lastBean.getCouponAmount()));
                        out.collect(bean);
                    }
                }
            });
    }
}
/*
读取 redis 和数据库, 都需要经过网络.
    网络的连接时间远远大于从 redis 和数据库查询的数据

以前的算子都是同步的

异步流处理:

如果要使用异步流处理, 那么外部系统的客户端要支持异步链接
    redis 和 phoenix 目前没有一部客户端可用
    
    使用多线程(线程池)+多客户端
        每一次连接, 创建一个线程, 在这个线程内创建一个客户端(同步)
        
     
 
 */
