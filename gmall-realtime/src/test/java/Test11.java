import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/11/12 14:25
 */
public class Test11 {
    public static void main(String[] args) {
        // 1999 / 1000
//        System.out.println((9 * 3600 * 1000 + 600 * 1000 + 10 -  8 * 3600 * 1000) / 1000 / 60 / 60);
    
    
        System.out.println(new BigDecimal(10).negate());
        System.out.println(new BigDecimal(0).subtract(new BigDecimal(10)));
    }
}
