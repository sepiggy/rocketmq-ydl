package cn.sepiggy.order.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

// 订单步骤实体类
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderStep implements Serializable {

    private long orderId; // 订单号
    private String desc; // 描述
}
