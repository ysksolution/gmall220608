package com.atguigu.gmall.gmallsugar.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface TradeMapper {
    // 读取指定日期总的销售额
    @Select("SELECT sum(order_amount)\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}")
    BigDecimal gmv(int date);  // 20221111
}
