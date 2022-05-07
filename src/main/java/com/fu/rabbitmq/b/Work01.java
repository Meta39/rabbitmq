package com.fu.rabbitmq.b;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 工作线程(消费者)
 */
public class Work01 {
    //队列名称
    public static final String QUEUE_NAME = "hello";

    //接收消息
    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();

        //声明    接受消息
        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println(new String(message.getBody()));
        };

        //取消消息时的回调
        CancelCallback cancelCallback = consumerTag ->{
            System.out.println("消息消费被中断");
        };

        /**
         * 消费者消费消息
         * 1.消费哪个队列
         * 2.是否自动应答
         * 3.接受消息回调
         * 4.取消消费回调
         */
        System.out.println("C1等待接收消息");
        channel.basicConsume(QUEUE_NAME,true,deliverCallback,cancelCallback);
    }
}
