package com.atguigu.gmall.realtime.app.dwd.db;

/**
 * @Author lzc
 * @Date 2022/11/9 14:23
 */
public class Dwd_07_DwdTradeRefundPaySuc {
    public static void main(String[] args) {
    
    }
}
/*
退款成功事务事实表
refund_payment:  update && data.refund_status=0705 && old.refund_status is not null
order_refund_info: update && data.refund_status=0705 && old.refund_status is not null
order_info:  update && data.order_status=1006 && old.order_status is not null
 
payment_type 维度退化
 */