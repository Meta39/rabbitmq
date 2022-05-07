package com.fu.rabbitmq.h;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 死信队列：成为条件——》消息被拒绝、消息TTL过期（时间）、队列达到最大长度
 * 消费者2
 */
public class Consumer02 {
    ///死信队列名称
    public static final String DEAD_QUEUE = "dead_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();

        System.out.println("等待接收消息.....");

        channel.basicConsume(DEAD_QUEUE,true, (consuumerTag,message) -> {
            System.out.println("Consumer02接收的消息是："+new String(message.getBody(), StandardCharsets.UTF_8));
        },consumerTag ->{});
    }
}
