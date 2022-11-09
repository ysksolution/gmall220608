package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/9 08:28
 */
public class Dwd_03_DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_03_DwdTradeOrderDetail().init(
            3003,
            2,
            "Dwd_03_DwdTradeOrderDetail"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取 ods_db 数据
        readOdsDb(tEnv, "Dwd_03_DwdTradeOrderDetail");
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 过滤 order_detail insert
        Table orderDetail = tEnv.sqlQuery("select " +
                                              "data['id'] id, " +
                                              "data['order_id'] order_id, " +
                                              "data['sku_id'] sku_id, " +
                                              "data['sku_name'] sku_name, " +
                                              "data['create_time'] create_time, " +
                                              "data['source_id'] source_id, " +
                                              "data['source_type'] source_type, " +
                                              "data['sku_num'] sku_num, " +
                                              "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                                              "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " + // 分摊原始总金额
                                              "data['split_total_amount'] split_total_amount, " +
                                              "data['split_activity_amount'] split_activity_amount, " +
                                              "data['split_coupon_amount'] split_coupon_amount, " +
                                              "ts, " +
                                              "pt " +
                                              "from ods_db " +
                                              "where `database`='gmall2022' " +
                                              "and `table`='order_detail' " +
                                              "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail", orderDetail);
        // 4. 过滤 order_info   insert
        Table orderInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id, " +
                                            "data['user_id'] user_id, " +
                                            "data['province_id'] province_id " +
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='order_info' " +
                                            "and `type`='insert' ");
        tEnv.createTemporaryView("order_info", orderInfo);
        // 5. 过滤 order_detail_activity   insert
        Table orderDetailActivity = tEnv.sqlQuery("select " +
                                                      "data['order_detail_id'] order_detail_id, " +
                                                      "data['activity_id'] activity_id, " +
                                                      "data['activity_rule_id'] activity_rule_id " +
                                                      "from ods_db " +
                                                      "where `database`='gmall2022' " +
                                                      "and `table`='order_detail_activity' " +
                                                      "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
        // 6. 过滤 order_detail_coupon   insert
        Table orderDetailCoupon = tEnv.sqlQuery("select " +
                                                    "data['order_detail_id'] order_detail_id, " +
                                                    "data['coupon_id'] coupon_id " +
                                                    "from ods_db " +
                                                    "where `database`='gmall2022' " +
                                                    "and `table`='order_detail_coupon' " +
                                                    "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        // 7. 把 5 张 join 到一起
        Table result = tEnv.sqlQuery("select " +
                                        "od.id, " +
                                        "od.order_id, " +
                                        "oi.user_id, " +
                                        "od.sku_id, " +
                                        "od.sku_name, " +
                                        "oi.province_id, " +
                                        "act.activity_id, " +
                                        "act.activity_rule_id, " +
                                        "cou.coupon_id, " +
                                        "date_format(od.create_time, 'yyyy-MM-dd') date_id, " + // 年月日
                                        "od.create_time, " +
                                        "od.source_id, " +
                                        "od.source_type, " +
                                        "dic.dic_name source_type_name, " +
                                        "od.sku_num, " +
                                        "od.split_original_amount, " +
                                        "od.split_activity_amount, " +
                                        "od.split_coupon_amount, " +
                                        "od.split_total_amount, " +
                                        "od.ts " +
                                        "from order_detail od " +
                                        "join order_info oi on od.order_id=oi.id " +
                                        "left join order_detail_activity act on od.id=act.order_detail_id " +
                                        "left join order_detail_coupon cou on od.id=cou.order_detail_id " +
                                        "join base_dic for system_time as of od.pt as dic " +
                                        "on od.source_type=dic.dic_code");
        result.execute().print();
    
        // 8. 创建一个动态表与 kafka 的 topic 关联
        
        // 9. join 的结果写入到 kafka 中
    }
}
/*
交易域下单事务事实表

会有哪些对应的事实表:
    order_detail
        新增若干数据
        join
    order_info
        下单动作, 会新增一条数据
        left join
    order_detail_activity
        新增若干数据
        left join
    order_detail_coupon
        新增若干数据
        
字典表:
    look up join
    
 

*/