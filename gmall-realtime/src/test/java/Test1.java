import net.minidev.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @Author lzc
 * @Date 2022/11/5 11:18
 */
public class Test1 {
    public static void main(String[] args) {
        JSONObject obj = new JSONObject();
        obj.put("a", 97);
        obj.put("b", 98);
        obj.put("c", 99);
    
        List<String> list = Arrays.asList("a,c".split(","));
    
        Set<String> keys = obj.keySet();
    
        // for 循环只能变量读取集合中的元素, 但是不能删除, 否则有异常:ConcurrentModificationException
        /*for (String key : keys) {
            if (!list.contains(key)) {
                keys.remove(key);
            }
        }*/
        /*while (it.hasNext()) {
            String key = it.next();
            if (!list.contains(key)) {
                it.remove();
            }
        }*/
    
        keys.removeIf(key -> !list.contains(key));
        System.out.println(obj);
    
    }
}
