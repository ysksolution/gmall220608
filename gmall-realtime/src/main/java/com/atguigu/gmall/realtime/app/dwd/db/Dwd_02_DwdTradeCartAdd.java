package com.atguigu.gmall.realtime.app.dwd.db;

/**
 * @Author lzc
 * @Date 2022/11/7 15:32
 */
public class Dwd_02_DwdTradeCartAdd {
    public static void main(String[] args) {
    
    }
}
/*


1. cdc 连不上 mysql
    jdk(1.8.3xxx) 的版本和 cdc 的版本一致
    更好 jdk 版本
2. job 打包 linux 的 yarn 执行
    报类型转换异常 ....
        依赖冲突
    解决办法:
        1. 打包的时候不打包 kafka 连接器或删除 lib 目录下的 kafka 连接器
        
        2. 提交 job 的时候, 添加一个参数: -Dclassloader.resolve-order=parent-first
*/