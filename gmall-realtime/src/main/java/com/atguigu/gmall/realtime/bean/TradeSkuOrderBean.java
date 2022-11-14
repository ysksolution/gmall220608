package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeSkuOrderBean {
    String orderDetailId; // 会引入一个 bug
    
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;
    // sku_id
    String skuId;
    // sku 名称
    String skuName;
    // spu_id
    String spuId;
    // spu 名称
    String spuName;
    
    // 原始金额
    @Builder.Default  // 给属性设置默认值
    BigDecimal originalAmount = new BigDecimal(0);
    // 活动减免金额
    @Builder.Default
    BigDecimal activityAmount = new BigDecimal(0);
    // 优惠券减免金额
    @Builder.Default
    BigDecimal couponAmount = new BigDecimal(0);
    // 下单金额
    @Builder.Default
    BigDecimal orderAmount = new BigDecimal(0);
    // 时间戳
    Long ts;
    
    public static void main(String[] args) {
        TradeSkuOrderBean bean = builder()
            .stt("abc")
            .category2Id("aaaa")
            .orderAmount(null)
            .build();
        
        System.out.println(bean);
    
//        System.out.println(new TradeSkuOrderBean());
    
    
    }
}