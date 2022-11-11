package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.bean.KeywordBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.IkAnalyzer;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/11 11:22
 */
public class Dws_01_DwsTrafficKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new Dws_01_DwsTrafficKeywordPageViewWindow().init(
            4001,
            2,
            "Dws_01_DwsTrafficKeywordPageViewWindow"
        );
        
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.建表通过 ddl: 与 dwd_traffic_page 关联
        tEnv.executeSql("create table dwd_traffic_page(" +
                            " page map<string, string>, " +
                            " ts bigint, " +
                            " et as to_timestamp_ltz(ts, 3), " +
                            " watermark for et as et - interval '3' second " +
                            ")" + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "Dws_01_DwsTrafficKeywordPageViewWindow"));
        
        
        // 2. 过滤出用户搜索记录, 得到搜索关键词
        // last_page_id = 'search' or 'home'
        // item_type = keyword
        // item is not null
        Table keywordTable = tEnv.sqlQuery("select " +
                                               " page['item'] keyword, " +
                                               " et " +
                                               "from dwd_traffic_page " +
                                               "where (page['last_page_id']='search' or page['last_page_id']='home') " +
                                               "and page['item_type']='keyword' " +
                                               "and page['item'] is not null");
        tEnv.createTemporaryView("keyword_table", keywordTable);
        // 3. 搜索记录关键词进行分词
        // 3.1 现在注册自定义函数
        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);
        // 3.2 在 sql 中使用
        Table kwTable = tEnv.sqlQuery("select " +
                                          " kw, " +
                                          " et " +
                                          "from keyword_table " +
                                          "join lateral table(ik_analyzer(keyword))as t(kw) on true");
        
        tEnv.createTemporaryView("kw_table", kwTable);
        // 3.开窗聚合
        Table resultTable = tEnv.sqlQuery("select" +
                                              " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                                              " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                                              " kw keyword, " +
                                              " count(*) keyword_count, " +
                                              " unix_timestamp() * 1000 ts " +  // count(*) sum(1) count(1) count(id)
                                              "from  table(tumble( table  kw_table, descriptor(et), interval '5' second )) " +
                                              "group by kw, window_start, window_end ");
        // 4. 写出到 clickhouse 中
        // 由于没有专用的 clickhouse 连接器, 所以需要自定义流式的连接器
        SingleOutputStreamOperator<KeywordBean> stream = tEnv
            .toRetractStream(resultTable, KeywordBean.class)
            .filter(t -> t.f0)  // 只要 true: 新增或更新后的数据
            .map(t -> t.f1);
        stream.print();
    
    
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
/*
分词
    分词器 ik 对字符串按照中文的习惯进行分词
"手机苹果手机256g白色苹果手机"
    手机
    苹果
    手机
    256g
    白色
    苹果
    手机

"手机 小米手机 256g白色小米手机"
...
"手机 华为手机"
...

 */