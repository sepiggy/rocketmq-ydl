package cn.sepiggy.order;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

// 消息顺序演示之消费者
public class Consumer {

    @SneakyThrows
    public static void main(String[] args) {
        // 1. 谁来收？
        val consumer = new DefaultMQPushConsumer("demo06-consumer-group-1");

        // 2. 从哪里收？
        consumer.setNamesrvAddr("localhost:9876");

        // 3. 监听哪个消息队列？
        consumer.subscribe("demo06-order", "*");

        // 4. 处理业务流程，注册监听器
        /**! MessageListenerConcurrently 并发监听, 不能保证消费顺序 **/
        /*
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messageList, ConsumeConcurrentlyContext context) {
                // 处理业务逻辑
                for (MessageExt msg : messageList) {
                    val body = new String(msg.getBody());
                    System.out.println("body = " + body);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        */

        /**! MessageListenerOrderly, 一个线程只监听一个 queue, 可以保证消费顺序 **/
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    val body = new String(msg.getBody());
                    System.out.println("body = " + body);
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();

        System.out.println("消费者启动成功");

        // 这里不需要关消费者, 因为消费者通过监听器启动了一个长连接监听 Broker
    }
}
