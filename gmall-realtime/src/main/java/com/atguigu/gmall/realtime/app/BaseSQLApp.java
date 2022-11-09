package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/4 14:05
 */
public abstract class BaseSQLApp {
    public void init(int port, int p, String ckAnAndJobName){
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
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall2022/" + ckAnAndJobName);
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
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 给 sql job 设置 name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckAnAndJobName);
        
        handle(env, tEnv);
        
    }
    
    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);
    
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId){
        tEnv.executeSql("create table ods_db(" +
                            " `database` string, " +
                            "  `table` string, " +
                            "  `type` string, " +
                            "  `ts` bigint, " +
                            "  `data` map<string, string>, " +
                            "  `old` map<string, string>, " +
                            "   pt as proctime() " +
                            ")" + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_ODS_DB, groupId));
        
    }
    
    public void readBaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql("create table base_dic(" +
                            " dic_code string, " +
                            " dic_name string " +
                            ") WITH (" +
                            "  'connector' = 'jdbc'," +
                            "  'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false'," +
                            "  'table-name' = 'base_dic', " +
                            "  'username' = 'root', " +
                            "  'password' = 'aaaaaa', " +
                            "  'lookup.cache.ttl' = '1 hour',  " +
                            "  'lookup.cache.max-rows' = '10'  " +
                            ")");
    }
    
    
}
