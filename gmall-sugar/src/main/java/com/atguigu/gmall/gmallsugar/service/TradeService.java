package com.atguigu.gmall.gmallsugar.service;

import com.atguigu.gmall.gmallsugar.bean.Province;
import com.atguigu.gmall.gmallsugar.bean.Spu;
import com.atguigu.gmall.gmallsugar.bean.Tm;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/11/18 09:25
 */
public interface TradeService {
    BigDecimal gmv(int date);
    
    List<Spu> gmvBySpu(int date);
    
    List<Tm> gmvByTm(int date);
    
    List<Province> province(int date);
}
