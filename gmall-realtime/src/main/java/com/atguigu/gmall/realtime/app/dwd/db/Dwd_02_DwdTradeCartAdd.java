package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
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
        
        // 2. 读取字典表的数据
        readBaseDic(tEnv);
        
        // 3. 过滤出 cart_info 的数据
        Table cartInfo = tEnv.sqlQuery("select " +
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
                                           " ts," +
                                           " pt " +
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
                                           ")");
        tEnv.createTemporaryView("cart_info", cartInfo);
        // 4. cart_info look up join 字典表
        Table result = tEnv.sqlQuery("select " +
                                         "ci.id, " +
                                         "ci.user_id, " +
                                         "ci.sku_id, " +
                                         "ci.source_id, " +
                                         "ci.source_type, " +
                                         "dic.dic_name source_type_name, " +
                                         "cast(ci.sku_num as string) sku_num, " +
                                         "ci.ts " +
                                         "from cart_info ci " +
                                         "join base_dic for system_time as of ci.pt as dic " +
                                         "on ci.source_type=dic.dic_code");
        // 3. 把加购事实表数据写入到 kafka 中
        tEnv.executeSql("create table dwd_trade_cart_add(" +
                            "id string, " +
                            "user_id string, " +
                            "sku_id string, " +
                            "source_id string, " +
                            "source_type_code string, " +
                            "source_type_name string, " +
                            "sku_num string, " +
                            "ts bigint " +
                            ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    
        result.executeInsert("dwd_trade_cart_add");
    }
}
/*
join 总结:
    join: 内连接
    left join: 左连接
        默认情况所有数据全部永远缓存到内存中. oom
            添加 ttl
        左连接:
            当左表数据先到: 会输出数据, 右表数据是 null, 当右来了之后,先删除刚才数据(kafka: 写入 null),
                再新增一条.
                
            写入到 kafka: 使用 upsert-kafka
            
            消费upsert-kafka的结果:
                null 值的处理
                    流:自定义反序列化器
                    sql: 不用额外处理
    lookup join
        专门用来在 sql 中 实时表去 join 维度表数据
            语法: 用的时态 join 的处理时间的语法
            1. 左表必须有处理时间字段
            2. 右表通过 jdbc 连接数据库
            
            3. ... for system_time as of left.pt as dic
            
------
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