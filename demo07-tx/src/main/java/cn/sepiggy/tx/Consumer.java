package cn.sepiggy.tx;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

// 事务消息演示之消费者
public class Consumer {

    @SneakyThrows
    public static void main(String[] args) {

        // 1. 谁来收？
        val consumer = new DefaultMQPushConsumer("consumer-group-1");

        // 2. 从哪里收？
        consumer.setNamesrvAddr("localhost:9876");

        // 3. 监听哪个消息队列？
        consumer.subscribe("demo07-tx", "*");

        // 4. 处理业务流程，注册监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 处理业务逻辑
                for (MessageExt msg : msgs) {
                    val body = new String(msg.getBody());
                    System.out.println("body = " + body);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("消费者启动成功");

        // 这里不需要关消费者, 因为消费者通过监听器启动了一个长连接监听 Broker
    }
}
