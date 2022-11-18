package com.atguigu.gmall.gmallsugar.service;


import com.atguigu.gmall.gmallsugar.bean.Traffic;
import com.atguigu.gmall.gmallsugar.mapper.TrafficMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficServiceImpl implements TrafficService{
    
    @Autowired
    TrafficMapper trafficMapper;
    @Override
    public List<Traffic> traffic(int date) {
        return trafficMapper.traffic(date);
    }
    
}
