package com.atguigu.gmall.gmallsugar.service;

import com.atguigu.gmall.gmallsugar.bean.Traffic;

import java.util.List;
import java.util.Map;

public interface TrafficService {
    List<Traffic> traffic(int date);
    
    
    List<Map<String, Object>> kw(int date);
}
