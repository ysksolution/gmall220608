package com.atguigu.gmall.realtime.function;

import com.atguigu.gmall.realtime.util.IkSplit;
import org.apache.flink.table.functions.TableFunction;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/11/11 13:57
 */
//@FunctionHint(output = @DataTypeHint("row<kw String>"))
public class IkAnalyzer extends TableFunction<String> {
    public void eval(String keyword){
        // 我是中国人
       List<String> kws =  IkSplit.split(keyword);
        for (String kw : kws) {
            collect(kw);
        }
       
    }
}
