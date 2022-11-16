package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/11/16 09:03
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(
            100, // 线程池核心线程数
            300, // 最大线程池
            60,  // 空闲线程存活时间
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>() // 当要提交的任务超过最大线程数的时候, 会把任务存储到这里
        );
    }
}
/*
new Thread(){ 覆写 run 方法}.start()

new Thread(new Runnable(){...}).start()



 */
