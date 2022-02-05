package cn.sepiggy.type;

import io.vavr.collection.List;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * 批量发送消息:
 * DefaultMQProducer#send(Collection)
 */
public class ProducerBatch {

    @SneakyThrows
    public static void main(String[] args) {

        val producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        val messageList = List.of(new Message("topic8", "tag1", "batch message1".getBytes()),
                new Message("topic8", "tag1", "batch message2".getBytes()),
                new Message("topic8", "tag1", "batch message3".getBytes())).toJavaList();
        val sendResult = producer.send(messageList);
        System.out.println("sendResult = " + sendResult);

//        producer.shutdown();
    }
}
