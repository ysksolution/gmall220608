package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import java.sql.*;
import java.util.ArrayList;
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
    public static <T> List<T> queryList(Connection conn,
                                        String querySql,
                                        Class<T> tClass,
                                        boolean ... isToCamel) {
        boolean flag = false;  // 默认下划线不转驼峰
        if (isToCamel.length > 0) { // 如果有传入, 则应用插入的值
            flag = isToCamel[0];
        }
        
        List<T> list = new ArrayList<>();
        try(PreparedStatement ps = conn.prepareStatement(querySql)) { // 1.7提供功能
    
            ResultSet resultSet = ps.executeQuery();
    
            ResultSetMetaData metaData = resultSet.getMetaData();
            
    
            while (resultSet.next()) {
                // 表示已经遍历到一行
                // 这个一行会有很多列, 把每列数据封装到一个对象中 T, 每列就是 T 中的一个属性
                T t = tClass.newInstance();  // 调用无参构造器创建对象
                // 每一行有多少列?
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    // 列名: T 的属性名
                    // 列值: T 的属性的值
                    String name = metaData.getColumnLabel(i);
                    if (flag) {
                        // 把 name 转成驼峰命名
                        // a_b aB
                        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                    }
                    Object obj = resultSet.getObject(i);
                    // t.name=obj
                    BeanUtils.setProperty(t, name, obj);
                }
    
                list.add(t);
            }
    
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    
    
        return list;
    }
    
    public static void main(String[] args) {
//        List<JSONObject> list = queryList(getMySqlConnection(), "select * from gmall_config.table_process ", JSONObject.class);
        List<TableProcess> list = queryList(getMySqlConnection(), "select * from gmall_config.table_process ", TableProcess.class, true);
        for (Object obj : list) {
    
            System.out.println(obj);
        }
    }
}
