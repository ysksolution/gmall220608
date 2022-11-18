package com.atguigu.gmall.gmallsugar.mapper;

import com.atguigu.gmall.gmallsugar.bean.Spu;
import com.atguigu.gmall.gmallsugar.bean.Tm;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface TradeMapper {
    // 读取指定日期总的销售额
    @Select("SELECT sum(order_amount)\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}")
    BigDecimal gmv(int date);  // 20221111
    
    @Select("SELECT\n" +
        "    spu_name,\n" +
        "    sum(order_amount) AS amount\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY spu_name")
    List<Spu> gmvBySpu(int date);  // 20221111
    
    @Select("SELECT\n" +
        "    trademark_name,\n" +
        "    sum(order_amount) AS amount\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY trademark_name")
    List<Tm> gmvByTm(int date);  // 20221111
    
}
