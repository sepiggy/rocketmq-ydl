package cn.sepiggy.tx;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

// 事务消息演示之生产者
public class Producer {

    @SneakyThrows
    public static void main(String[] args) {

        // 1. 谁来发? 事务消息生产者
        val transactionMQProducer = new TransactionMQProducer("producer-group-1");

        // 2. 发给谁?
        transactionMQProducer.setNamesrvAddr("localhost:9876");

        // 设置事务监听器
        transactionMQProducer.setTransactionListener(new TransactionListener() {
            // 处理本地事务
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                // 把消息保存到 mysql 数据库
                // 根据入库是否成功返回
                // 若成功 -> 返回 LocalTransactionState.COMMIT_MESSAGE,
                // 若失败 -> 返回 LocalTransactionState.ROLLBACK_MESSAGE
                //! if (数据库执行sql成功) {
                //!   return LocalTransactionState.COMMIT_MESSAGE; // 正常提交
                //! } else if (数据库执行sql失败) {
                //!   return LocalTransactionState.ROLLBACK_MESSAGE; // 回滚
                //! } else { // 未知
                //!   return LocalTransactionState.UNKNOW;
                //! }

                /* 模拟本地事务正常提交 */
//                System.out.println("模拟数据库执行事务正常提交");
//                return LocalTransactionState.COMMIT_MESSAGE;

                /* 模拟本地事务回滚 */
//                System.out.println("模拟数据库执行事务回滚");
//                return LocalTransactionState.ROLLBACK_MESSAGE;

                /* 模拟事务补偿, 配合 checkLocalTransaction 一起使用 */
                System.out.println("模拟数据库执行事务未知");
                return LocalTransactionState.UNKNOW;
            }

            // 处理事务补偿
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("模拟事务补偿");
                //! if (再次到数据库检查之前的sql执行成功) {
                //!     return LocalTransactionState.COMMIT_MESSAGE;
                //! } else if (再次到数据库检查之前的sql执行失败) {
                //!     return LocalTransactionState.ROLLBACK_MESSAGE;
                //! } else { // 未知
                //!     再次状态未知，需要运维人员手工介入
                //! }
//                return LocalTransactionState.COMMIT_MESSAGE; // 事务补偿成功
                return LocalTransactionState.ROLLBACK_MESSAGE; // 事务补偿失败
            }
        });


        transactionMQProducer.start();

        // 3. 怎么发?
        // 4. 发什么?
        val body = "hello world with rocketmq tx";
        val message = new Message("demo07-tx", "demo07-tx-tag-1", body.getBytes());
        // 发送事务消息
        val transactionSendResult = transactionMQProducer.sendMessageInTransaction(message, null);

        // 5. 发的结果是什么?
        System.out.println("transactionSendResult = " + transactionSendResult);

        // 发送事务消息不要关生产者，不需要打扫战场
    }
}
