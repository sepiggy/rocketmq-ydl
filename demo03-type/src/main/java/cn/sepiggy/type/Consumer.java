package cn.sepiggy.type;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class Consumer {

    @SneakyThrows
    public static void main(String[] args) {
        // 1. 谁来收？
        val consumer = new DefaultMQPushConsumer("group1");
//        val consumer = new DefaultMQPushConsumer("group2");

        // 2. 从哪里收？
        consumer.setNamesrvAddr("localhost:9876");

        // 设置消费者的消费模式
        // 默认使用的是 MessageModel.CLUSTERING 模式 (负载均衡模式)
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 也可以设置为广播模式
//        consumer.setMessageModel(MessageModel.BROADCASTING);

        // 3. 监听哪个消息队列？
//        consumer.subscribe("topic2", "*");
//        consumer.subscribe("topic3", "*");
        consumer.subscribe("topic4", "*");

        // 4. 处理业务流程，注册监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 处理业务逻辑
                for (MessageExt msg : msgs) {
                    val body = new String(msg.getBody());
                    System.out.println("body = " + body);
                    System.out.println("======================================================");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("消费者启动成功");

        // 这里不需要关消费者, 因为消费者通过监听器启动了一个长连接监听 Broker
    }
}
