package cn.sepiggy.one2many;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

// 一个生产者多个消费者 (one2many)
// 消费者分为负载均衡模式和广播模式

// 负载均衡模式：多个消费者会竞争拿消息
// 方式1) 指定相同的 consumerGroup
// 方式2) consumer.setMessageModel(MessageModel.CLUSTERING);

// 广播模式：多个消费者会拿到相同的消息
// 方式1) 指定不同的 consumerGroup
// 方式2) consumer.setMessageModel(MessageModel.BROADCASTING);

// 总结：
// 1) 不同的消费者组会拿到相同的消息，根据设置消费者是否属于同一组可以实现是负载均衡模式还是广播模式
// 2) 相同的消费者组内，根据消费模型也可以实现是负载均衡模式还是广播模式

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
