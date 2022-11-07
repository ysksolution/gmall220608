package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/11/7 09:21
 */
@Slf4j
public class Dwd_01_DwdBaseLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_01_DwdBaseLogApp().init(
            3001,
            2,
            "Dwd_01_DwdBaseLogApp",
            Constant.TOPIC_ODS_LOG
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        //        1. 读取 ods_log 数据
        //        2. 对数据做 etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        //        3. 纠正新老客户
        validateNewOrOld(etledStream).print();
        //        4. 分流
        //        5. 不同的流的数据写入到不同的 topic 种
    }
    
    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> stream) {
        /*
        如何纠正新老客户标记: is_new
            错误: 把老用户识别成了新用户
                当时新用户的时候才有必要纠正
                
         
         6 号
         
         7 号
         
         保存一个状态: 存储时当前 mid 的首次访问的日期 年月日
         
         is_new=1  才有必要纠正
            state != null  // 表示以前访问过
            
                判断这条数据的日期与状态是否一致, 如果一致
                    不用纠正
                
                如果日期不一致
                    要纠正
            state == null
                首次访问
                    不应纠正
                    
                    把日期存入到状态中
         
        
         */
        return stream
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .map(new RichMapFunction<JSONObject, JSONObject>() {
                
                private ValueState<String> firstVisitDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateState", String.class));
                }
                
                @Override
                public JSONObject map(JSONObject obj) throws Exception {
                    
                    // 获取的当前 mid 首次的访问日期
                    String firstVisitDate = firstVisitDateState.value();
                    // 获取当前的数据的日期
                    Long ts = obj.getLong("ts");
                    String today = AtguiguUtil.tsToDate(ts);
    
                    JSONObject common = obj.getJSONObject("common");
                    String isNew = common.getString("is_new");
                    if ("1".equals(isNew)) {
                        if (firstVisitDate == null) {
                           // 首次访问
                            // 更新状态
                            firstVisitDateState.update(today);
                        }else if(!today.equals(firstVisitDate)){
                            // is_new 有误: 应该是 0
                            common.put("is_new", "0");
                        }
                    }else{
                        // 老用户: 如果状态是 null, 应该把状态设置一个比较早的日期: 比如昨天
                        if (firstVisitDate == null) {
                            String yesterday = AtguiguUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                            firstVisitDateState.update(yesterday);
                        }
                    }
                    return obj;
                }
            });
        
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    try {
                        JSON.parseObject(value);
                    } catch (Exception e) {
                        log.warn("日志格式不对, 不是合法的 json 日志: " + value);
                        return false;
                    }
                    return true;
                }
            })
            .map(JSON::parseObject);
    }
}
/*
流量域
	ods_log
		所有的日志数据都在这个一个 topic 种

		5 种日志:
			启动
			曝光
			错误
			活动
			页面
		dwd层的任务:
			不同的日志写入到不同的 topic 种: dwd 层的数据

			分流, 不同的流的数据写入到不同的 topic 种

			手动分流 利用 if 语句进行分流
				缺点: 不够灵活

			5 个流:
				 4 个测输出流

	任务分解:
		1. 读取 ods_log 数据
		2. 对数据做 etl
		3. 纠正新老客户
		4. 分流
		5. 不同的流的数据写入到不同的 topic 种
 */