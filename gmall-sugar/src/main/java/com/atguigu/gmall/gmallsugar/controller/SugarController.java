package com.atguigu.gmall.gmallsugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmallsugar.bean.Province;
import com.atguigu.gmall.gmallsugar.bean.Spu;
import com.atguigu.gmall.gmallsugar.bean.Tm;
import com.atguigu.gmall.gmallsugar.bean.Traffic;
import com.atguigu.gmall.gmallsugar.service.TradeService;
import com.atguigu.gmall.gmallsugar.service.TrafficService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    
    
    @RequestMapping("/sugar/gmv/tm")
    public String gmvByTm(Integer date) {
        // date 是 null, 表示要查询当天
        if (date == null) {
            
            date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        
        // 从 service 读取数据
        
        List<Tm> list = tradeService.gmvByTm(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        JSONArray data = new JSONArray();
        for (Tm tm : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", tm.getTrademark_name());
            obj.put("value", tm.getAmount());
            data.add(obj);
        }
        result.put("data", data);
        return result.toJSONString();
    }
    
    
    @Autowired
    TrafficService trafficService;
    
    @RequestMapping("/sugar/traffic")
    public String traffic(Integer date) {
        // date 是 null, 表示要查询当天
        if (date == null) {
            
            date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        
        // 从 service 读取数据
        List<Traffic> list = trafficService.traffic(date);
        // 把 list 集合值, 转到 Map集合中: hour->Traffic
        HashMap<Integer, Traffic> hourToTraffic = new HashMap<>();
        for (Traffic traffic : list) {
            hourToTraffic.put(traffic.getHour(), traffic);
        }
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        JSONObject data = new JSONObject();
        
        JSONArray categories = new JSONArray();
        for (int hour = 0; hour < 24; hour++) {
            categories.add(hour);
        }
        data.put("categories", categories);
        
        JSONArray series = new JSONArray();
        
        JSONObject pv = new JSONObject();
        pv.put("name", "pv");
        JSONArray pvData = new JSONArray();
        pv.put("data", pvData);
        series.add(pv);
        
        JSONObject uv = new JSONObject();
        uv.put("name", "uv");
        JSONArray uvData = new JSONArray();
        uv.put("data", uvData);
        series.add(uv);
        
        JSONObject sv = new JSONObject();
        sv.put("name", "sv");
        JSONArray svData = new JSONArray();
        sv.put("data", svData);
        series.add(sv);
        
        
        for (int hour = 0; hour < 24; hour++) {
            Traffic traffic = hourToTraffic.getOrDefault(hour, new Traffic(hour, 0L, 0L, 0L));
            pvData.add(traffic.getPv());
            uvData.add(traffic.getUv());
            svData.add(traffic.getSv());
        }
        
        data.put("series", series);
        
        
        result.put("data", data);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/trade/province")
    public String province(Integer date) {
        // date 是 null, 表示要查询当天
        if (date == null) {
            
            date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        
        // 从 service 读取数据
        List<Province> list = tradeService.province(date);
        
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        JSONObject data = new JSONObject();
        
        JSONArray mapData = new JSONArray();
    
        for (Province province : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", province.getProvince_name());
            obj.put("value", province.getOrder_amount());
    
            JSONArray tooltipValues = new JSONArray();
            tooltipValues.add(province.getOrder_count());
            obj.put("tooltipValues", tooltipValues);
    
            mapData.add(obj);
        }
        
        data.put("mapData", mapData);
        
        data.put("valueName", "销售额");
        
        JSONArray tooltipNames = new JSONArray();
        tooltipNames.add("订单数");
        data.put("tooltipNames", tooltipNames);
        
        JSONArray tooltipUnits = new JSONArray();
        tooltipUnits.add("个");
        data.put("tooltipUnits", tooltipUnits);
        
        
        result.put("data", data);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/trade/kw")
    public String kw(Integer date) {
        // date 是 null, 表示要查询当天
        if (date == null) {
            
            date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        
        // 从 service 读取数据
        List<Map<String, Object>> list = trafficService.kw(date);
    
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        JSONArray data = new JSONArray();
        for (Map<String, Object> map : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", map.get("keyword"));
            obj.put("value", map.get("ct"));
            data.add(obj);
        }
        result.put("data", data);
        
        return result.toJSONString();
    }
}

