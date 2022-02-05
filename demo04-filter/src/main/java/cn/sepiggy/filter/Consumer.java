package cn.sepiggy.filter;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

// 演示消费者对消息过滤, eg. 只接收消息队列中的偶数
// 1) 使用 tag 进行消息过滤
// 通过 DefaultMQPushConsumer#subscribe(topic, MessageSelector.byTag(tag) 第二个参数控制过滤哪些消息
// tag 中写过滤的标签
// 2) 使用 sql 进行消息过滤
// 通过 DefaultMQPushConsumer#subscribe(topic, MessageSelector.bySql(sql) 第二个参数控制过滤哪些消息
// sql 中写过滤的 sql 语句
public class Consumer {

    @SneakyThrows
    public static void main(String[] args) {
        val consumer = new DefaultMQPushConsumer("group1");

        consumer.setNamesrvAddr("localhost:9876");

        // 1) 使用 tag 方式进行消息过滤
//        consumer.subscribe("filter-topic-tag", MessageSelector.byTag("偶数"));

        // 2) 使用 sql 方式进行消息过滤
        consumer.subscribe("filter-topic-sql", MessageSelector.bySql("even = 0"));

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
