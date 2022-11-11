package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/11/11 14:03
 */
public class IkSplit {
    public static void main(String[] args) {
//        List<String> list = split("我是中国人");
        List<String> list = split("手机 华为手机 华为 256G 手机");
        for (String s : list) {
            System.out.println(s);
    
        }
    }
    
    // 使用 ik 分词器, 对传入的字符串进行分词
    public static List<String> split(String keyword) {
    
        HashSet<String> set = new HashSet<>();        /// String -> Reader
        // 内存流
        StringReader reader = new StringReader(keyword);
        // 参数二: 布尔值 是否使用智能模式
        IKSegmenter seg = new IKSegmenter(reader, true);
        
        // 这个方法调用得到切开的词
        try {
            Lexeme next = seg.next();
            while (next != null) {
                String s = next.getLexemeText();
                set.add(s);
                next = seg.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new ArrayList<>(set);
    }
}
