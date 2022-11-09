package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/9 14:23
 */
public class Dwd_07_DwdTradeRefundPaySuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_07_DwdTradeRefundPaySuc().init(
            3007,
            2,
            "Dwd_07_DwdTradeRefundPaySuc"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取 ods_db 数据
        readOdsDb(tEnv, "Dwd_07_DwdTradeRefundPaySuc");
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 从 ods_db 中过滤 refund_payment
        Table refundPayment = tEnv.sqlQuery("select " +
                                                "data['id'] id, " +
                                                "data['order_id'] order_id, " +
                                                "data['sku_id'] sku_id, " +
                                                "data['payment_type'] payment_type, " +
                                                "data['callback_time'] callback_time, " +
                                                "data['total_amount'] total_amount, " +
                                                //                                                "data['refund_status'] refund_status, " +
                                                "pt, " +
                                                "ts " +
                                                "from ods_db " +
                                                "where `database`='gmall2022' " +
                                                "and `table`='refund_payment' "
                                            //                          +
                                            //                          "and `type`='update' " +
                                            //                          "and `old`['refund_status'] is not null " +
                                            //                          "and `data`['refund_status']='0705' "
        );
        tEnv.createTemporaryView("refund_payment", refundPayment);
        // 4. 从 ods_db 中过滤order_refund_info
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                                                  "data['order_id'] order_id, " +
                                                  "data['sku_id'] sku_id, " +
                                                  "data['refund_num'] refund_num, " +
                                                  "`old` " +
                                                  "from ods_db " +
                                                  "where `database`='gmall2022' " +
                                                  "and `table`='order_refund_info' "
                                              //                          +
                                              //                          "and `type`='update' " +
                                              //                          "and `old`['refund_status'] is not null " +
                                              //                          "and `data`['refund_status']='0705' "
        );
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        // 5. 从 ods_db 中过滤order_info
        Table refundOrderInfo = tEnv.sqlQuery("select " +
                                                  "data['id'] id, " +
                                                  "data['user_id'] user_id, " +
                                                  "data['province_id'] province_id, " +
                                                  "`old` " +
                                                  "from ods_db " +
                                                  "where `database`='gmall2022' " +
                                                  "and `table`='order_info' " +
                                                  "and `type`='update' " +
                                                  "and `old`['order_status'] is not null " +
                                                  "and `data`['order_status']='1006' "
        );
        tEnv.createTemporaryView("refund_order_info", refundOrderInfo);
        
        // 6. 4 张表进行 join
        Table result = tEnv.sqlQuery("select " +
                                        "rp.id, " +
                                        "oi.user_id, " +
                                        "rp.order_id, " +
                                        "rp.sku_id, " +
                                        "oi.province_id, " +
                                        "rp.payment_type, " +
                                        "dic.dic_name payment_type_name, " +
                                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id, " +
                                        "rp.callback_time, " +
                                        "ri.refund_num, " +
                                        "rp.total_amount, " +
                                        "rp.ts " +
                                        "from refund_payment rp " +
                                        "join order_refund_info ri " +
                                        "on rp.order_id=ri.order_id and rp.sku_id=ri.sku_id " +
                                        "join refund_order_info oi on  rp.order_id=oi.id " +
                                        "join base_dic for system_time as of rp.pt as dic " +
                                        "on rp.payment_type=dic.dic_code ");
        
    
        // 7. 写出到 kafka 中
        tEnv.executeSql("create table dwd_trade_refund_pay_suc( " +
                                "id string, " +
                                "user_id string, " +
                                "order_id string, " +
                                "sku_id string, " +
                                "province_id string, " +
                                "payment_type_code string, " +
                                "payment_type_name string, " +
                                "date_id string, " +
                                "callback_time string, " +
                                "refund_num string, " +
                                "refund_amount string, " +
                                "ts bigint " +
                                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC));
    
        result.executeInsert("dwd_trade_refund_pay_suc");
        
    }
}
/*
退款成功事务事实表
refund_payment:  update && data.refund_status=0705 && old.refund_status is not null
order_refund_info: update && data.refund_status=0705 && old.refund_status is not null
order_info:  update && data.order_status=1006 && old.order_status is not null
 
payment_type 维度退化
 */