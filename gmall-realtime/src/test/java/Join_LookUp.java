import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/8 14:36
 */
public class Join_LookUp extends BaseSQLApp {
    public static void main(String[] args) {
        new Join_LookUp().init(6001,1,"Join_LookUp");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table base_dic(" +
                            " dic_code string, " +
                            " dic_name string " +
                            ") WITH (" +
                            "  'connector' = 'jdbc'," +
                            "  'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false'," +
                            "  'table-name' = 'base_dic', " +
                            "  'username' = 'root', " +
                            "  'password' = 'aaaaaa', " +
                            // 时间设置多长合适?
                            // 在性能和准确性做一个平衡
                            "  'lookup.cache.ttl' = '60 second',  " +
                            "  'lookup.cache.max-rows' = '10'  " +
                            ")");
        
        // 事实表:必须有处理时间字段
        tEnv.executeSql("create table abc(" +
                            "id string, " +
                            "pt as proctime() " +
                            ")" + SQLUtil.getKafkaSourceDDL("abc", "abc", "csv"));
    
        tEnv.sqlQuery("select" +
                          " abc.id, " +
                          " dic.dic_name " +
                          "from abc " +
                          "join base_dic for system_time as of abc.pt as dic " +
                          "on abc.id=dic.dic_code ")
            .execute()
            .print();
    }
}
