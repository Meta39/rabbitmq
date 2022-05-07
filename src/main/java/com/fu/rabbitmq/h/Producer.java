package com.fu.rabbitmq.h;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 死信队列
 * 生产者
 */
public class Producer {

    //普通交互机名称
    public static final String NORMAL_EXCHANGE = "normal_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();
        //死信消息  设置TTL时间     单位是ms 10000ms=10s
        //*** AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().expiration("10000").build();

        for (int i = 0; i < 11; i++) {
            String message = "我是第"+i+"条消息";
            //*** channel.basicPublish(NORMAL_EXCHANGE,"zhangsan",properties,message.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish(NORMAL_EXCHANGE,"zhangsan",null,message.getBytes(StandardCharsets.UTF_8));
        }
    }
}
