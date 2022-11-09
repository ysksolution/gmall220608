import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/8 10:22
 */
public class Consume_Kafka_SQL_2 extends BaseSQLApp {
    
    public static void main(String[] args) {
        new Consume_Kafka_SQL_2().init(5002, 1, "Consume_Kafka_SQL_1");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // sql 消费的时候: 不用考虑 value 是 null 的情况, 直接会忽略
        tEnv.executeSql("create table t4(" +
                            " id int, " +
                            " name string, " +
                            " age int," +
                            " primary key(id) not enforced " +
                            ")" + SQLUtil.getUpsetKafkaDDL("t4"));
        
        tEnv.sqlQuery("select * from t4").execute().print();
        
        
        
        
    }
}
/*
常规 join:
    默认情况所有数据都保存在状态中, 永久的保存
    
    一定要设置 ttl
 */