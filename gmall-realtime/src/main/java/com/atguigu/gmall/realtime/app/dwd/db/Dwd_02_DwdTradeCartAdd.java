package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/7 15:32
 */
public class Dwd_02_DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_02_DwdTradeCartAdd().init(3002, 2, "Dwd_02_DwdTradeCartAdd");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 通过 ddl 建立动态表与 ods_db 关联, 从这个 topic 进行读取数据
        readOdsDb(tEnv, "Dwd_02_DwdTradeCartAdd");
        
        
        // 2. 过滤出 cart_info 的数据
        tEnv.sqlQuery("select " +
                          " `data`['id'] id, " +
                          " `data`['user_id'] user_id, " +
                          " `data`['sku_id'] sku_id, " +
                          " `data`['cart_price'] cart_price, " +
                          " if(`type`='insert', " +
                          "   cast(`data`['sku_num'] as int), " +
                          "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                          " ) sku_num, " +
                          " `data`['source_id'] source_id," +
                          " `data`['source_type'] source_type, " +
                          " ts " +
                          "from ods_db " +
                          "where `database`='gmall2022' " +
                          "and `table`='cart_info' " +
                          "and " +
                          "(" +
                          "   `type`='insert'" +
                          "    or " +
                          "    (`type`='update' " +
                          "      and `old`['sku_num'] is not null " +
                          "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)" +
                          "    )" +
                          ")").execute().print();
        
        // 3. 把加购事实表数据写入到 kafka 中
    }
}
/*
维度退化:
用到 sql 的 join


----
过滤加购数据: 新增和更新

如果是更新的话:
    sku_num 变化, 变大
    
   `old`['sku_num'] is not null  sku_num 发生了变化
   data[sku-num] > old[sku_num]
   
   id  sku_id sku_num
   1      1      2    新增  -> 2
   1      1      6    更新  -> 6 - 2 = 4
   
   select  sum(sku_num) from a group by sku_id
   

------
加购事务事实表
数据源:
    从 ods_db 里面的 cart_info
    
业务:
    新增的加购
    变化
        只关注 sku_num 增加的情况
        
    最后写入到 kafka 中
    
流的方式处理? sql 的方式?
    sql
        业务数据, mysql, 结构化的数据
    
    流
        处理半结构或非结构化数据



-----------
1. cdc 连不上 mysql
    jdk(1.8.3xxx) 的版本和 cdc 的版本一致
    更好 jdk 版本
2. job 打包 linux 的 yarn 执行
    报类型转换异常 ....
        依赖冲突
    解决办法:
        1. 打包的时候不打包 kafka 连接器或删除 lib 目录下的 kafka 连接器
        
        2. 提交 job 的时候, 添加一个参数: -Dclassloader.resolve-order=parent-first
*/