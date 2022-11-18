package com.atguigu.gmall.gmallsugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmallsugar.bean.Spu;
import com.atguigu.gmall.gmallsugar.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/11/18 09:54
 */
@RestController
public class SugarController {
    @Autowired
    TradeService tradeService;
    // https://natapp.cn/#
    @RequestMapping("/sugar/gmv")
    public String gmv(Integer date) {
        // date 是 null, 表示要查询当天
        if (date == null) {
            
            date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        
        // 从 service 读取数据
        BigDecimal gmv = tradeService.gmv(date);
        
        JSONObject result = new JSONObject();
        
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", gmv);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/gmv/spu")
    public String gmvBySpu(Integer date) {
        // date 是 null, 表示要查询当天
        if (date == null) {
            
            date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        
        // 从 service 读取数据
        List<Spu> list = tradeService.gmvBySpu(date);
    
        JSONObject result = new JSONObject();
        
        result.put("status", 0);
        result.put("msg", "");
    
        JSONObject data = new JSONObject();
    
        JSONArray categories = new JSONArray();
        for (Spu spu : list) {
            categories.add(spu.getSpu_name());
        }
        data.put("categories", categories);
    
        JSONArray series = new JSONArray();
        JSONObject obj = new JSONObject();
        obj.put("name", "spu名字");
        JSONArray data1 = new JSONArray();
        for (Spu spu : list) {
            data1.add(spu.getAmount());
        }
        obj.put("data", data1);
        series.add(obj);
        data.put("series", series);
    
        result.put("data", data);
    
    
        return result.toJSONString();
    }
    
}
