package com.atguigu.gmall.realtime.function;


import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * @Author lzc
 * @Date 2022/11/16 09:23
 */
public class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) {
        
    }
}
