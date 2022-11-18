package com.atguigu.gmall.gmallsugar;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall.gmallsugar.mapper")
public class GmallSugarApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallSugarApplication.class, args);
    }

}
