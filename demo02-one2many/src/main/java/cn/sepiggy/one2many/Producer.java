package cn.sepiggy.one2many;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

// 一个生产者多个消费者
// 演示负载均衡模式
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
        for (int i = 0; i < 10; i++) {
            val body = "hello world rocketmq" + i;
//            val message = new Message("topic2", "tag1", body.getBytes());
//            val message = new Message("topic3", "tag1", body.getBytes());
            val message = new Message("topic4", "tag1", body.getBytes());
            val sendResult = producer.send(message);
            // 5. 发的结果是什么?
            System.out.println("sendResult = " + sendResult);
        }

        // 6. 打扫战场?
        producer.shutdown();
    }
}
