import com.atguigu.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/11/9 10:57
 */
public class Dwd_05_DwdTradePayDetailSuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_05_DwdTradePayDetailSuc().init(
            3005,
            2,
            "Dwd_05_DwdTradePayDetailSuc"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(1820));
        // 1. 读取 ods_db
        readOdsDb(tEnv, "Dwd_05_DwdTradePayDetailSuc");
      
 
        tEnv.createTemporaryFunction("map_contain", MapContains.class);
        // 4. 从 ods_db 中过滤payment_info
        Table paymentInfo = tEnv.sqlQuery("select " +
                                        "data['user_id'] user_id, " +
                                        "data['order_id'] order_id, " +
                                        "data['payment_type'] payment_type, " +
                                        "data['callback_time'] callback_time, " +
                                        "`old`['callback_time'] ot, " +
                                        "`old` olddd, " +
                                        "ts, " +
                                        "pt " +
                                        "from ods_db " +
                                        "where `database`='gmall2022' " +
                                        "and `table`='payment_info' " +
                                        "and `type`='update' " +
                                        "and  map_contain(`old`, 'callback_time')");
        tEnv.createTemporaryView("payment_info", paymentInfo);
    
        tEnv.toAppendStream(paymentInfo, Row.class).print();
    
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    
    }
}
/*
支付成功事务事实表
payment_info  当支付成功 insert
dwd_trade_order_detail 获取到产品相关信息


字典表: payment_type 做维度退化

 */