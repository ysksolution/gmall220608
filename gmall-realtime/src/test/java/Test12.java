import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/14 11:45
 */
public class Test12 {
    @AllArgsConstructor
    @Data
    public static class Person {
        public int a;
    }
    
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        env.fromElements(new Person(10), new Person(20))
            .keyBy(p -> "aa")
            .map(new RichMapFunction<Person, Person>() {
            
                private ValueState<Person> state;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    state = getRuntimeContext().getState(new ValueStateDescriptor<Person>("state", Person.class));
                }
            
                @Override
                public Person map(Person value) throws Exception {
                
                    if (state.value() == null) {
                    
                        state.update(value);
                    } else {
                        System.out.println(state.value());
                    }
                    
                
                    value.a = 1000;
                
                    return value;
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
}
