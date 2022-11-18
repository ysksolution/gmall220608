package com.atguigu.gmall.gmallsugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/11/18 10:30
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Tm {
    private String trademark_name;
    private BigDecimal amount;

}