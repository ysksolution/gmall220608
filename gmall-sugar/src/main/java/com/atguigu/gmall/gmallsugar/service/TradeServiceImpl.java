package com.atguigu.gmall.gmallsugar.service;

import com.atguigu.gmall.gmallsugar.bean.Province;
import com.atguigu.gmall.gmallsugar.bean.Spu;
import com.atguigu.gmall.gmallsugar.bean.Tm;
import com.atguigu.gmall.gmallsugar.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

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
    
    @Override
    public List<Spu> gmvBySpu(int date) {
        return tradeMapper.gmvBySpu(date);
    }
    
    @Override
    public List<Tm> gmvByTm(int date) {
        return tradeMapper.gmvByTm(date);
    }
    
    @Override
    public List<Province> province(int date) {
        return tradeMapper.province(date);
    }
    
}
