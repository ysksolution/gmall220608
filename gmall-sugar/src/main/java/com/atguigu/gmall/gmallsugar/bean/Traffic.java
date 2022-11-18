package com.atguigu.gmall.gmallsugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/11/18 10:30
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Traffic {
    private Integer hour;
    private Long uv;
    private Long sv;
    private Long pv;

}
