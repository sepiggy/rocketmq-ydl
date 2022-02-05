package cn.sepiggy.type;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

// 演示同步、异步、单向、延时消息
// 同步消息：DefaultMQProducer#send(msg)
// 异步消息：DefaultMQProducer#send(msg, sendCallback)
// 单向消息：DefaultMQProducer#sendOneway
// 延时消息：Message#setDelayTimeLevel
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
        /**
         * 1) 同步消息
         * 通过 producer#send(msg) 发送同步消息
         */
        /*
        for (int i = 0; i < 10; i++) {
            val body = "hello world rocketmq" + i;
//            val message = new Message("topic2", "tag1", body.getBytes());
//            val message = new Message("topic3", "tag1", body.getBytes());
            val message = new Message("topic4", "tag1", body.getBytes());
            // 发送同步消息
            val sendResult = producer.send(message);
            // 5. 发的结果是什么?
            System.out.println("sendResult = " + sendResult);

            System.out.println("同步消息发送完成");
        }
        */

        /**
         * 2) 异步消息
         * 通过 producer#send(msg, sendCallback) 发送异步消息
         */
        /*
        for (int i = 0; i < 10; i++) {
            val body = "hello world rocketmq" + i;
            val message = new Message("topic5", "tag1", body.getBytes());
            // 发送异步消息
            producer.send(message, new SendCallback() {
                // 5. 发的结果是什么?
                // 发送成功的回调方法
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("sendResult = " + sendResult);
                }

                // 发送失败的回调方法
                @Override
                public void onException(Throwable e) {
                    System.out.println("e = " + e);
                }
            });

            System.out.println("异步消息发送完成");
        }
        */

        /**
         * 3) 单向消息
         * 通过 producer#sendOneway 发送单向消息
         */
        /*
        for (int i = 0; i < 10; i++) {
            val body = "hello world rocketmq" + i;
            val message = new Message("topic6", "tag1", body.getBytes());
            producer.sendOneway(message);
            System.out.println("单向消息发送完成");
        }
        */

        /**
         * 4) 延时消息
         */
        /*
        for (int i = 0; i < 10; i++) {
            val body = "hello world rocketmq" + i;
            val message = new Message("topic7", "tag1", body.getBytes());
            // 延时消息，可以分别设置每条消息的延时等级
            message.setDelayTimeLevel(3);
            val sendResult = producer.send(message);
            System.out.println("sendResult = " + sendResult);
            System.out.println("延时消息发送完成");
        }
        */

        // 6. 打扫战场，异步消息不能 shutdown，需要等待 broker 的回调函数
//        producer.shutdown();
    }
}
