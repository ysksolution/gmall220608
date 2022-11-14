package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/13 09:13
 */
public class Dws_09_DwsTradeSkuOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow().init(
            4009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 按照 order_detail_id 去重
        distinctByOrderDetailId(stream);
    }
    
    private void distinctByOrderDetailId(DataStreamSource<String> stream) {
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
                                
                    第二条数据来:
                            100 有值  null -> a(取出状态中的值): -100 有值 null
                                              b: 100 有值 有值
                            
                 
                 
                    
                 
        
        
         */
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
