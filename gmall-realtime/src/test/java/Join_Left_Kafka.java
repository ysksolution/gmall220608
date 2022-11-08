import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/8 10:22
 */
public class Join_Left_Kafka extends BaseSQLApp {
    public static void main(String[] args) {
        new Join_Left_Kafka().init(5001, 2, "Join_1");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // join 的时候, 这种数据在状态中保存的时间
       // left Join的时候: 左表不会过时, 右表也会过时
        // left join 左边的数据如果 10s 没有 join 才会过时  右表只要到了 10s 一定过时
//        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        
        tEnv.executeSql("create table t1(" +
                            "id int, " +
                            "name string " +
                            ")" + SQLUtil.getKafkaSourceDDL("t1", "Join_1", "csv"));
    
        tEnv.executeSql("create table t2(" +
                            "id int, " +
                            "age int " +
                            ")" + SQLUtil.getKafkaSourceDDL("t2", "Join_1", "csv"));
    
        Table result = tEnv.sqlQuery("select " +
                                        "t1.id, " +
                                        "name, " +
                                        "age " +
                                        "from t1 " +
                                        "left join t2 on t1.id=t2.id");
    
        tEnv.executeSql("create table t4(" +
                            " id int, " +
                            " name string, " +
                            " age int," +
                            " primary key (id) not enforced" +
                            ")" + SQLUtil.getUpsetKafkaDDL("t4"));
    
        result.executeInsert("t4");
    }
}
/*

常规 join:
    默认情况所有数据都保存在状态中, 永久的保存
    一定要设置 ttl
 */