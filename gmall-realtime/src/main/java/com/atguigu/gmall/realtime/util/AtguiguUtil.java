package com.atguigu.gmall.realtime.util;

import java.text.SimpleDateFormat;

/**
 * @Author lzc
 * @Date 2022/11/7 10:29
 */
public class AtguiguUtil {
    public static void main(String[] args) {
    
    }
    
    public static String tsToDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
    
}
