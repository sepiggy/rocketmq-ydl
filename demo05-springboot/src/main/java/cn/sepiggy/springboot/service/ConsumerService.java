package cn.sepiggy.springboot.service;

import cn.sepiggy.springboot.domain.User;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * 消费者
 */
@Service
// 基本使用
// @RocketMQMessageListener(topic = "sb-rocketmq-topic", consumerGroup = "consumer-group-1")

// 使用 tag 过滤 （默认过滤方式，SelectorType 不指定就是使用 tag 过滤）
//@RocketMQMessageListener(
//        topic = "sb-rocketmq-topic",
//        consumerGroup = "consumer-group-1",
//        selectorType = SelectorType.TAG,
//        selectorExpression = "tag1 || tag2")

// 使用 sql 过滤
//@RocketMQMessageListener(
//        topic = "sb-rocketmq-topic",
//        consumerGroup = "consumer-group-1",
//        selectorType = SelectorType.SQL92,
//        selectorExpression = "age > 92")

// 修改消费模式
@RocketMQMessageListener(
        topic = "sb-rocketmq-topic",
        consumerGroup = "consumer-group-1",
        messageModel = MessageModel.BROADCASTING
)
public class ConsumerService implements RocketMQListener<Object> {

    // 接收消息的业务逻辑
    @Override
    public void onMessage(Object object) {
//        System.out.println("user = " + user);
        System.out.println("object = " + object);
    }
}
