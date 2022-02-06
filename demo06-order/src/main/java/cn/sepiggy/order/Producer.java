package cn.sepiggy.order;

import cn.sepiggy.order.domain.OrderStep;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

// 消息顺序演示之生产者
public class Producer {

    @SneakyThrows
    public static void main(String[] args) {

        // 1. 谁来发?
        val producer = new DefaultMQProducer("producer-group-1");

        // 2. 发给谁?
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        // 3. 怎么发?
        // 4. 发什么?
        // 模拟订单数据，其顺序为：创建->付款->推送->完成
        val orderStepList = io.vavr.collection.List.of(
                new OrderStep(1L, "创建"),
                new OrderStep(2L, "创建"),
                new OrderStep(1L, "付款"),
                new OrderStep(3L, "创建"),
                new OrderStep(2L, "付款"),
                new OrderStep(3L, "付款"),
                new OrderStep(2L, "完成"),
                new OrderStep(1L, "推送"),
                new OrderStep(3L, "完成"),
                new OrderStep(1L, "完成")
        ).toJavaList();

        /*! 错误的发送方式，没有保障消息的顺序性 */
        /*
        for (OrderStep orderStep : orderStepList) {
            val msg = new Message("demo06-order", "demo06-order-tag-1", orderStep.toString().getBytes());
            val sendResult = producer.send(msg);
            // 5. 发的结果是什么?
            System.out.println("sendResult = " + sendResult);
        }
        */

        /*! 正确的发送方式，使用队列选择器，保障消息的顺序性 */
        for (OrderStep orderStep : orderStepList) {
            val msg = new Message("demo06-order", "demo06-order-tag-1", orderStep.toString().getBytes());
            val sendResult = producer.send(msg, new MessageQueueSelector() {
                //! 队列选择的方式:
                //! 同一笔订单的相关操作保证放到同一个队列
                //! orderId 相同的消息即为同一笔订单，将其放进同一个队列，通过取模操作保证
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    // 队列数
                    val messageQueueSize = mqs.size();
                    val orderId = (int) (orderStep.getOrderId());
                    return mqs.get(orderId % messageQueueSize);
                }
            }, null);
            // 5. 发的结果是什么?
            System.out.println("sendResult = " + sendResult);
        }

        // 6. 打扫战场
        producer.shutdown();
    }
}
