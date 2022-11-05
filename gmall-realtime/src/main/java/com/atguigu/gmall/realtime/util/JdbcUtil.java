package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/11/5 09:04
 */
public class JdbcUtil {
    
    public static Connection getPhoenixConnection() {
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;
    
        return getJdbcConnection(driver, url, null, null);
    }
    
    public static Connection getJdbcConnection(String driver,
                                               String url,
                                               String user,
                                               String password){
        // 1. 加载驱动
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    
        // 2. 获取连接
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static Connection getMySqlConnection() {
        String driver = Constant.MYSQL_DRIVER;
        String url = Constant.MYSQL_URL;
    
        return getJdbcConnection(driver, url, "root", "aaaaaa");
    }
    
    // 泛型方法
    public static <T> List<T> queryList(Connection conn, String querySql, Class<T> tClass) {
        return null;
    }
}
