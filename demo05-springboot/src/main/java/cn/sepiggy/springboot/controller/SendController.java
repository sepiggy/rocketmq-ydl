package cn.sepiggy.springboot.controller;

import cn.sepiggy.springboot.domain.User;
import lombok.val;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

/**
 * 生产者
 */
@RestController
@RequestMapping("/send")
public class SendController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("/str")
    public String sendStr() {
        val msgBody = "hello rocketmq with springboot";
        rocketMQTemplate.convertAndSend("sb-rocketmq-topic", msgBody);
        return "success";
    }

    @GetMapping("/user")
    public String sendUser() {
        val user = new User("sepiggy", 100);
        rocketMQTemplate.convertAndSend("sb-rocketmq-topic", user);
        return "success";
    }

    // 同步消息
    @GetMapping("/sync")
    public String sendSync() {
        val user = new User("sepiggy", new Random(System.currentTimeMillis()).nextInt(100));
        val sendResult = rocketMQTemplate.syncSend("sb-rocketmq-topic", user);
        System.out.println("sendResult = " + sendResult);
        return "success";
    }

    // 异步消息
    @GetMapping("async")
    public String sendAsync() {
        val user = new User("sepiggy", new Random(System.currentTimeMillis()).nextInt(100));
        rocketMQTemplate.asyncSend("sb-rocket-topic", user, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("sendResult = " + sendResult);
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("e = " + e);
            }
        }, 1000);

        return "success";
    }

    // 单向消息
    @GetMapping("oneway")
    public String sendOneway() {
        val user = new User("sepiggy", new Random(System.currentTimeMillis()).nextInt(100));
        rocketMQTemplate.sendOneWay("sb-rocket-topic", user);
        return "success";
    }

    // 延时消息
    @GetMapping("delay")
    public String sendDelay() {
        val user = new User("sepiggy", new Random(System.currentTimeMillis()).nextInt(100));
        rocketMQTemplate.syncSend("sb-rocket-topic", MessageBuilder.withPayload(user).build(), 2000, 3);
        return "success";
    }
}
