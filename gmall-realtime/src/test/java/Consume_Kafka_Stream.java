import com.atguigu.gmall.realtime.app.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/8 11:32
 */
public class Consume_Kafka_Stream extends BaseAppV1 {
    public static void main(String[] args) {
        new Consume_Kafka_Stream().init(20000, 2,"Consume_Kafka_Stream","t4");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        stream.print();
    }
}
