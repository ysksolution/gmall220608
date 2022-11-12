package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/11/12 08:26
 */
@Slf4j
public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().init(
            4002,
            2,
            "Dws_02_DwsTrafficVcChArIsNewPageViewWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 数据清洗
        SingleOutputStreamOperator<String> etledStream = etl(stream);
        // 2. 把数据封装到一个 pojo 中
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = parseToPojo(etledStream);
        // 3. 开窗集合
        SingleOutputStreamOperator<TrafficPageViewBean> resultStream = windowAndAgg(beanStream);
        // 4. 写出到 clickhouse 中
        writeToClickhouse(resultStream);
        
    }
    
    private void writeToClickhouse(SingleOutputStreamOperator<TrafficPageViewBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHouseSink("dws_traffic_vc_ch_ar_is_new_page_view_window", TrafficPageViewBean.class));
    }
    
    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
      return  beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
                    // 可以避免由于数据倾斜导致的水印不更新问题
                    .withIdleness(Duration.ofSeconds(20))
            )
            // keyBy 的 key 永远用字符串
            .keyBy(bean -> bean.getAr() + "_" + bean.getCh() + "_" + bean.getIsNew() + "_" + bean.getVc())
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean bean1,
                                                      TrafficPageViewBean bean2) throws Exception {
                        bean1.setDurSum(bean1.getDurSum() + bean2.getDurSum());
                        bean1.setUvCt(bean1.getUvCt() + bean2.getUvCt());
                        bean1.setPvCt(bean1.getPvCt() + bean2.getPvCt());
                        bean1.setSvCt(bean1.getSvCt() + bean2.getSvCt());
                        
                        return bean1;
                    }
                },
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        // 存储的时候前面聚和的最终结果: 有且仅有一个值
                                        Iterable<TrafficPageViewBean> elements,
                                        Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.tsToDateTime(ctx.window().getEnd()));
    
                        // 把 ts 改成统计时间
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);
    
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(SingleOutputStreamOperator<String> stream) {
        return stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                
                private ValueState<String> visitDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    visitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitDateState", String.class));
                }
                
                @Override
                public TrafficPageViewBean map(JSONObject obj) throws Exception {
                    Long ts = obj.getLong("ts");
                    JSONObject common = obj.getJSONObject("common");
                    JSONObject page = obj.getJSONObject("page");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");
                    
                    
                    //TODO
                    // 是否是当天当前 mid 的第一条记录
                    Long uvCt = 0L;
                    // 这条数据的年月日与状态是否一致: 一致就是当天的第一条, 否则就不是当天的第一条
                    String today = AtguiguUtil.tsToDate(ts);
                    String visitDate = visitDateState.value();
                    if (!today.equals(visitDate)) { // 状态不一致
                        uvCt = 1L;
                        visitDateState.update(today);
                    }
                    
                    Long svCt = 0L;
                    String lastPageId = page.getString("last_page_id");
                    if (lastPageId == null || lastPageId.length() == 0) {
                        svCt = 1L;
                    }
                    
                    Long pvCt = 1L;
                    Long durSum = page.getLong("during_time");
                    
                    
                    return new TrafficPageViewBean(
                        "", "",
                        vc, ch, ar, isNew,
                        uvCt, svCt, pvCt, durSum,
                        ts
                    );
                }
            });
    }
    
    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try {
                    JSON.parseObject(value);
                } catch (Exception e) {
                    log.warn("数据格式不是合法的 json: " + value);
                    return false;
                }
                
                return true;
            }
        });
    }
}
/*
窗口处理函数:
   
    
    增量
        简单
            sum max min
            maxBy minBy
        复杂
            reduce
            
            aggregate
    全量
        process
        

-------
版本-渠道-地区-访客类别
数据源: 页面日志

页面浏览数
    pv
浏览总时长
    sum(during_time)
会话数
    找到这样的页面: 建立 session 的页面
    last_page_id = null
独立访客数
    (以一天为限制)
    uv
    
    用一个状态记录当前用户是否访问过, 如果是当天第一次访问, 则对 uv 贡献1 否则就是 0
        状态保存一个年月日
        
        今天是一次
            状态为 null
                把日期存入到状态中
        明天第一次
            状态是 11 月 12 日, 日期不一致
            uv贡献 1, 把状态更新为 11 月 13 日
*/