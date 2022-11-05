/**
 * @Author lzc
 * @Date 2022/11/5 09:19
 */
public class Test {
    public static void main(String[] args) {
        System.out.println("id,name,region_id,area_code,iso_code,iso_3166_2"
                               .replaceAll("[^,]+","$0 varchar"));
    }
}
