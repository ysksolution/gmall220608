package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class Dws_11_DwsTradeTrademarkCategoryUserRefundWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_11_DwsTradeTrademarkCategoryUserRefundWindow().init(
            4010,
            2,
            "Dws_11_DwsTradeTrademarkCategoryUserRefundWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> streamWithoutDimId = stream
            .map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                @Override
                public TradeTrademarkCategoryUserRefundBean map(String value) throws Exception {
                    JSONObject obj = JSON.parseObject(value);
                    return TradeTrademarkCategoryUserRefundBean.builder()
                        .skuId(obj.getString("sku_id"))
                        .userId(obj.getString("user_id"))
                        .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                        .ts(obj.getLong("ts") * 1000)
                        .build();
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            );
        
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> streamWithTmIdAndC3Id = AsyncDataStream.unorderedWait(
            streamWithoutDimId,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                protected String getTable() {
                    return "dim_sku_info";
                }
                
                @Override
                protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getSkuId();
                }
                
                @Override
                protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                    input.setTrademarkId(dim.getString("TM_ID"));
                    input.setCategory3Id(dim.getString("CATEGORY3_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> streamWithC3 = AsyncDataStream.unorderedWait(
            streamWithTmIdAndC3Id,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                protected String getTable() {
                    return "dim_base_category3";
                }
                
                @Override
                protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getCategory3Id();
                }
                
                @Override
                protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                    input.setCategory3Name(dim.getString("NAME"));
                    input.setCategory2Id(dim.getString("CATEGORY2_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> streamWithC2 = AsyncDataStream.unorderedWait(
            streamWithC3,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                protected String getTable() {
                    return "dim_base_category2";
                }
                
                @Override
                protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getCategory2Id();
                }
                
                @Override
                protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                    input.setCategory2Name(dim.getString("NAME"));
                    input.setCategory1Id(dim.getString("CATEGORY1_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> stream1 = streamWithC2
            .keyBy(bean -> bean.getCategory1Id() +
                "_" + bean.getCategory2Id() +
                "_" + bean.getCategory3Id() +
                "_" + bean.getTrademarkId() +
                "_" + bean.getUserId())
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1,
                                                                       TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<TradeTrademarkCategoryUserRefundBean> elements,
                                        Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                        
                        TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();
                        bean.setStt(AtguiguUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(ctx.window().getEnd()));
                        
                        bean.setRefundCount((long) bean.getOrderIdSet().size());
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                        
                    }
                }
            );
        
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> stream2 = AsyncDataStream.unorderedWait(
            stream1,
            new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                @Override
                protected String getTable() {
                    return "dim_base_trademark";
                }
                
                @Override
                protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                    return input.getTrademarkId();
                }
                
                @Override
                protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                    input.setTrademarkName(dim.getString("TM_NAME"));
                    
                }
            },
            60,
            TimeUnit.SECONDS
        );
    
    
        AsyncDataStream
            .unorderedWait(
                stream2,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_category1";
                    }
                
                    @Override
                    protected String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }
                
                    @Override
                    protected void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                        input.setCategory1Name(dim.getString("NAME"));
                    
                    }
                },
                60,
                TimeUnit.SECONDS
            )
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_trademark_category_user_refund_window", TradeTrademarkCategoryUserRefundBean.class));
        
    }
}
