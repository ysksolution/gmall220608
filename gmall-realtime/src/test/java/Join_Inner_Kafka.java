import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/8 10:22
 */
public class Join_Inner_Kafka extends BaseSQLApp {
    public static void main(String[] args) {
        new Join_Inner_Kafka().init(5001, 2, "Join_1");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // join 的时候, 这种数据在状态中保存的时间
        // 时间怎么设置? 根据表数据生成的关系来定.
        // order_info  order_detail  下单: 同时在两张表生成数据  ttl 5s
        // order_info  order_detail  payment_info 订单支付: ttl 30m+5s
//        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(20));
        
        tEnv.executeSql("create table t1(" +
                            "id int, " +
                            "name string " +
                            ")" + SQLUtil.getKafkaSourceDDL("t1", "Join_1"));
    
        tEnv.executeSql("create table t2(" +
                            "id int, " +
                            "age int " +
                            ")" + SQLUtil.getKafkaSourceDDL("t2", "Join_1"));
    
        Table result = tEnv.sqlQuery("select " +
                                        "t1.id, " +
                                        "name, " +
                                        "age " +
                                        "from t1 " +
                                        "join t2 on t1.id=t2.id");
        
        tEnv.executeSql("create table t3(" +
                            " id int, " +
                            " name string, " +
                            " age int " +
                            ")" + SQLUtil.getKafkaSinkDDL("t3"));
    
        result.executeInsert("t3");
    
    }
}
/*
常规 join:
    默认情况所有数据都保存在状态中, 永久的保存
    
    一定要设置 ttl
 */