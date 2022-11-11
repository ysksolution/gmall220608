import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/11/11 10:54
 */
public class MapContains extends ScalarFunction {
   public Boolean eval(Map<String, String> map, String key){
       return map.containsKey(key);
   }
}
