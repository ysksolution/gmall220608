package com.atguigu.gmall.gmallsugar.service;

import com.atguigu.gmall.gmallsugar.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/11/18 09:26
 */
@Service
public class TradeServiceImpl implements TradeService{
    @Autowired
    TradeMapper tradeMapper;
    @Override
    public BigDecimal gmv(int date) {
        return tradeMapper.gmv(date);
    }
    
}
