package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
import java.sql.Connection;
import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/11/13 09:13
 */
public class Dws_09_DwsTradeSkuOrderWindow_1 extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_1().init(
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
    
    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim) {
        /*
        流中的 join: 两个流, 事实表数据
        
        
        现在需要时事实表与维度表进行 join
            不能使用流与流的 join
            
        flink sql: 事实表与维度表 join 用 lookup join
        
        流与维度表的 join: 需要自己实现
            每来一条数据, 根据需要的条件去查询对应的维度信息: 本质执行一个 sql 语句
        
         */
        beanStreamWithoutDim
            .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                
                private Connection conn;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 1. 创建到 phoenix 的链接
                    conn = DruidDSUtil.getPhoenixConn();
                }
                
                @Override
                public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                    
                    // 1.sku_info
                    JSONObject skuInfo = DimUtil.readDimFromPhoenix(conn, "dim_sku_info", bean.getSkuId());
                    bean.setSkuName(skuInfo.getString("SKU_NAME"));
                    
                    bean.setSpuId(skuInfo.getString("SPU_ID"));
                    bean.setTrademarkId(skuInfo.getString("TM_ID"));
                    bean.setCategory3Id(skuInfo.getString("CATEGORY3_ID"));
                    
                    // 2. spu_info
                    JSONObject spuInfo = DimUtil.readDimFromPhoenix(conn, "dim_spu_info", bean.getSpuId());
                    bean.setSpuName(spuInfo.getString("SPU_NAME"));
                    
                    // 3. base_trademark
                    JSONObject baseTrademark = DimUtil.readDimFromPhoenix(conn, "dim_base_trademark", bean.getTrademarkId());
                    bean.setTrademarkName(baseTrademark.getString("TM_NAME"));
                    
                    // 4. c3
                    JSONObject c3 = DimUtil.readDimFromPhoenix(conn, "dim_base_category3", bean.getCategory3Id());
                    bean.setCategory3Name(c3.getString("NAME"));
                    bean.setCategory2Id(c3.getString("CATEGORY2_ID"));
                    
                    // 5. c2
                    JSONObject c2 = DimUtil.readDimFromPhoenix(conn, "dim_base_category2", bean.getCategory2Id());
                    bean.setCategory2Name(c2.getString("NAME"));
                    bean.setCategory1Id(c2.getString("CATEGORY1_ID"));
                    
                    // 6. c1
                    JSONObject c1 = DimUtil.readDimFromPhoenix(conn, "dim_base_category1", bean.getCategory1Id());
                    bean.setCategory1Name(c1.getString("NAME"));
                    
                    return bean;
                }
            })
            .print();
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
SKU粒度下单各窗口
------------

sku_id_1    0-5    1000   100 200
sku_id_2    0-5    ..  ...
sku_id_3    0-5    ..  ...

sku_id_1    5-10    1000   100 200
sku_id_2    5-10    ..  ...
sku_id_3    5-10    ..  ...

keyBy:sku_id -> 开窗聚合

----------
1. 数据源 dwd_trade_order_detail

2. 数据有重复
    order_detail_id  sku_id   分摊总金额   活动表
    1                   1       100       null
    null
    1                   1       100       有值
    
    keyBy:sku_id -> 分摊总金额被重复计算
    
    去重目标: 保留最后一个
    
2. 把数据封装到 pojo 中

3. 开窗聚合
         keyBy:sku_id
         
4. 补充维度信息
         需要 sku_name  spu_id spu_name base_category1  base_category2  base_category3  base_trademark
         
         根据 sku_id 查找其他对应的维度信息
         
         执行 sql 语句
         
5. 最后写入到 clickhouse 中
        

    

    



 */
