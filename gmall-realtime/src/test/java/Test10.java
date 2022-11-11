import com.atguigu.gmall.realtime.bean.KeywordBean;

import java.lang.reflect.Field;

/**
 * @Author lzc
 * @Date 2022/11/11 15:38
 */
public class Test10 {
    public static void main(String[] args) throws NoSuchFieldException, InstantiationException, IllegalAccessException {
        Class<KeywordBean> tClass = KeywordBean.class;
        
        //Field[] fields = tClass.getDeclaredFields();
        
        /*String m = "";
        for (Field field : fields) {
            m += field.getName() + ",";
        }
        m = m.substring(0, m.length() - 1);
        System.out.println(m);*/
        
        //String s = Arrays.stream(fields).map(f -> f.getName()).collect(Collectors.joining(","));
        // System.out.println(s);
        Object bean = tClass.newInstance();
        Field stt = tClass.getDeclaredField("stt");
        stt.setAccessible(true);// 让这个属性允许访问
        stt.set(bean, "abc");  // bean.stt="abc"
    
        System.out.println(bean);
    
        System.out.println(stt.get(bean));
    
    
    }
}
