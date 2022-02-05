package cn.sepiggy.filter;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

// 演示消息过滤
public class Producer {

    @SneakyThrows
    public static void main(String[] args) {
        val producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        // 1) 演示使用 tag 进行消息过滤
/*
        for (int i = 0; i < 1000; i++) {
            Message message;
            if (i % 2 != 0) {
                message = new Message("filter-topic-tag", "奇数", String.valueOf(i).getBytes());
            } else {
                message = new Message("filter-topic-tag", "偶数", String.valueOf(i).getBytes());
            }
            val sendResult = producer.send(message);
            System.out.println("sendResult = " + sendResult);
        }
*/

        // 2) 演示使用 sql 进行消息过滤
        for (int i = 0; i < 1000; i++) {
            val message = new Message("filter-topic-sql", "defaultTag", String.valueOf(i).getBytes());
            if (i % 2 != 0) {
                message.putUserProperty("even", "1");
            } else {
                message.putUserProperty("even", "0");
            }
            val sendResult = producer.send(message);
            System.out.println("sendResult = " + sendResult);
        }

        producer.shutdown();
    }
}
