package cn.sepiggy.simple;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

// 发送消息
public class Producer {

    @SneakyThrows
    public static void main(String[] args) {

        // 1. 谁来发?
        val producer = new DefaultMQProducer("group1");

        // 2. 发给谁?
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        // 3. 怎么发?
        // 4. 发什么?
        val body = "hello world rocketmq";
        val message = new Message("topic1", "tag1", body.getBytes());
        val sendResult = producer.send(message);

        // 5. 发的结果是什么?
        System.out.println("sendResult = " + sendResult);

        // 6. 打扫战场
        producer.shutdown();
    }
}
