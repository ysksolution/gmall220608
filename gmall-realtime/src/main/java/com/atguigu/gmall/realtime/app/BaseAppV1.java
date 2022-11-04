package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/4 14:05
 */
public abstract class BaseAppV1 {
    public void init(int port, int p, String ckAndGroupId, String topic){
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // 从 kafka 读取topi ods_db 数据
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
    
        // 1. 开启 checkpoint
        env.enableCheckpointing(3000);
        // 2. 设置状态后端: 使用状态状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 3. 设置 checkpoint 的存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall2022/" + ckAndGroupId);
        // 4. 设置 checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        // 5. 设置 checkpoint 的模式: 严格一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 6. 设置 checkpoint 的并发数,
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 7. 设置 checkpoint 之间的时间最小间隔. 如果设置了这个, 则setMaxConcurrentCheckpoints可以省略
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 8. 设置当 job 取消的时候, 是否保留 checkpoint 的数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    
        // 从 topic 读取数据
        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic));
        
        // 根据得到的流, 完成业务逻辑
        handle(env, stream);
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    
    }
    
    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> stream);
}
