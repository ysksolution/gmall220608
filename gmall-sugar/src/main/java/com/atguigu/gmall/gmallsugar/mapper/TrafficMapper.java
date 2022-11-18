package com.atguigu.gmall.gmallsugar.mapper;

import com.atguigu.gmall.gmallsugar.bean.Traffic;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/11/18 11:15
 */
public interface TrafficMapper {
    @Select("SELECT\n" +
        "    toHour(stt) AS hour,\n" +
        "    sum(uv_ct) AS uv,\n" +
        "    sum(sv_ct) AS sv,\n" +
        "    sum(pv_ct) AS pv\n" +
        "FROM dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY toHour(stt)")
    List<Traffic> traffic(int date);
    
    @Select("SELECT\n" +
        "    keyword,\n" +
        "    sum(keyword_count) ct \n" +
        "FROM dws_traffic_keyword_page_view_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY keyword")
    List<Map<String, Object>> kw(int date);
    
}
