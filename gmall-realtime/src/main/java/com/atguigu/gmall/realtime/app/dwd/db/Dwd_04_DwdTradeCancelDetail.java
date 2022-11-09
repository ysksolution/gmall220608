package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/11/9 10:01
 */
public class Dwd_04_DwdTradeCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_04_DwdTradeCancelDetail().init(
            3004,
            2,
            "Dwd_04_DwdTradeCancelDetail"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // order 取消订单
        // dwd_trade_order_detail 下单
        // 取消订单从业务上落后下单最大 30m + 网络延迟5s + dwd_trade_order_detail罗盘延迟 5s
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 10));
        
        // 1. 读取 ods_db 数据
        readOdsDb(tEnv,"Dwd_04_DwdTradeCancelDetail");
        
        // 2. 过滤出 order_info   update and old['order_status'] is null and data['order_status']='1003'
        Table orderInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id, " +
                                            "data['user_id'] user_id, " +
                                            "data['province_id'] province_id, " +
                                            "data['operate_time'] operate_time, " +
                                            "ts " +  // 订单取消的时间
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='order_info' " +
                                            "and `type`='update' " +
                                            "and `old`['order_status'] is not null " +
                                            "and `data`['order_status']='1003'");
        tEnv.createTemporaryView("order_info", orderInfo);
        // 3. 读取 下单事务事实表 dwd_trade_order_detail
        tEnv.executeSql("create table dwd_trade_order_detail(" +
                            "id string, " +
                            "order_id string, " +
                            "user_id string, " +
                            "sku_id string, " +
                            "sku_name string, " +
                            "province_id string, " +
                            "activity_id string, " +
                            "activity_rule_id string, " +
                            "coupon_id string, " +
                            "date_id string, " +
                            "create_time string, " +
                            "source_id string, " +
                            "source_type string, " +
                            "source_type_name string, " +
                            "sku_num string, " +
                            "split_original_amount string, " +
                            "split_activity_amount string, " +
                            "split_coupon_amount string, " +
                            "split_total_amount string " +
                            ")" + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, "Dwd_04_DwdTradeCancelDetail"));
        // 4. order_info 和dwd_trade_order_detail进行 join
        Table result = tEnv.sqlQuery("select " +
                                        "od.id, " +
                                        "od.order_id, " +
                                        "oi.user_id, " +
                                        "od.sku_id, " +
                                        "od.sku_name, " +
                                        "oi.province_id, " +
                                        "od.activity_id, " +
                                        "od.activity_rule_id, " +
                                        "od.coupon_id, " +
                                        "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                                        "oi.operate_time, " +
                                        "od.source_id, " +
                                        "od.source_type, " +
                                        "od.source_type_name, " +
                                        "od.sku_num, " +
                                        "od.split_original_amount, " +
                                        "od.split_activity_amount, " +
                                        "od.split_coupon_amount, " +
                                        "od.split_total_amount, " +
                                        "oi.ts " +
                                        "from order_info oi " +
                                        "join dwd_trade_order_detail od " +
                                        "on oi.id=od.order_id");
        // 5. 写出到 kafka 中
        tEnv.executeSql("create table dwd_trade_cancel_detail( " +
                                "id string, " +
                                "order_id string, " +
                                "user_id string, " +
                                "sku_id string, " +
                                "sku_name string, " +
                                "province_id string, " +
                                "activity_id string, " +
                                "activity_rule_id string, " +
                                "coupon_id string, " +
                                "date_id string, " +
                                "cancel_time string, " +
                                "source_id string, " +
                                "source_type_code string, " +
                                "source_type_name string, " +
                                "sku_num string, " +
                                "split_original_amount string, " +
                                "split_activity_amount string, " +
                                "split_coupon_amount string, " +
                                "split_total_amount string, " +
                                "ts bigint " +
                                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CANCEL_DETAIL));
    
        result.executeInsert("dwd_trade_cancel_detail");
        
    }
}
/*
取消订单事务事实表
涉及到哪些事实表?
    order_info   update and old['order_status'] is null and data['order_status']='1003'
    order_detail   insert
    order_detail_activity insert
    order_detail_coupon insert
        直接使用下单事务事实表来代替这 3 张表
        
     其实是 2 张
        order_info   update and old['order_status'] is null and data['order_status']='1003'
        dwd_trade_order_detail ...
        
        有没有重复数据?
            有
            
 字典表也需要?
    不需要. 退化字段在dwd_trade_order_detail已经退化过.
 
 
    
    


 */