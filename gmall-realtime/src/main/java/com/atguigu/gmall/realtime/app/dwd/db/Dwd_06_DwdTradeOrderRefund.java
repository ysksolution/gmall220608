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
 * @Date 2022/11/9 13:48
 */
public class Dwd_06_DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_06_DwdTradeOrderRefund().init(
            3006,
            2,
            "Dwd_06_DwdTradeOrderRefund"
        );
        
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        
        // 1. 读取 ods_db
        readOdsDb(tEnv, "Dwd_06_DwdTradeOrderRefund");
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 过滤order_refund_info
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                                                  "data['id'] id, " +
                                                  "data['user_id'] user_id, " +
                                                  "data['order_id'] order_id, " +
                                                  "data['sku_id'] sku_id, " +
                                                  "data['refund_type'] refund_type, " +
                                                  "data['refund_num'] refund_num, " +
                                                  "data['refund_amount'] refund_amount, " +
                                                  "data['refund_reason_type'] refund_reason_type, " +
                                                  "data['refund_reason_txt'] refund_reason_txt, " +
                                                  "data['create_time'] create_time, " +
                                                  "pt, " +
                                                  "ts " +
                                                  "from ods_db " +
                                                  "where `database`='gmall2022' " +
                                                  "and `table`='order_refund_info' " +
                                                  "and `type`='insert' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        
        // 4. 从 order_info 中过滤退单信息
        Table refundOrderInfo = tEnv.sqlQuery("select " +
                                                  "data['id'] id," +
                                                  "data['province_id'] province_id," +
                                                  "`old` " +
                                                  "from ods_db " +
                                                  "where `database`='gmall2022' " +
                                                  "and `table`='order_info' " +
                                                  "and `type`='update' " +
                                                  "and `old`['order_status'] is not null " +
                                                  "and `data`['order_status']='1005' ");
        tEnv.createTemporaryView("refund_order_info", refundOrderInfo);
        // 5. 4 张表 join
        Table result = tEnv.sqlQuery("select " +
                                        "ri.id, " +
                                        "ri.user_id, " +
                                        "ri.order_id, " +
                                        "ri.sku_id, " +
                                        "oi.province_id, " +
                                        "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
                                        "ri.create_time, " +
                                        "ri.refund_type, " +
                                        "dic1.dic_name refund_type_name, " +
                                        "ri.refund_reason_type, " +
                                        "dic2.dic_name refund_reason_type_name, " +
                                        "ri.refund_reason_txt, " +
                                        "ri.refund_num, " +
                                        "ri.refund_amount, " +
                                        "ri.ts  " +
                                        "from order_refund_info ri " +
                                        "join refund_order_info oi on ri.order_id=oi.id " +
                                        "join base_dic for system_time as of ri.pt as dic1 " +
                                        "on ri.refund_type=dic1.dic_code " +
                                        "join base_dic for system_time as of ri.pt as dic2 " +
                                        "on ri.refund_reason_type=dic2.dic_code ");
        
        // 6. 写出到 kafka 中
        tEnv.executeSql("create table dwd_trade_order_refund( " +
                                "id string, " +
                                "user_id string, " +
                                "order_id string, " +
                                "sku_id string, " +
                                "province_id string, " +
                                "date_id string, " +
                                "create_time string, " +
                                "refund_type_code string, " +
                                "refund_type_name string, " +
                                "refund_reason_type_code string, " +
                                "refund_reason_type_name string, " +
                                "refund_reason_txt string, " +
                                "refund_num string, " +
                                "refund_amount string, " +
                                "ts bigint " +
                                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));
        result.executeInsert("dwd_trade_order_refund");
    }
}
/*
退单事务事实表
order_refund_info: insert 数据  粒度: sku_id
order_info: update order_status=1005

字典表
    refund_type
    refund_reason

*/