package com.fu.rabbitmq.e;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 交换机：消费者
 */
public class ReceiveLogs01 {

    //交换机名称
    public static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();
        //声明一个交换机
        channel.exchangeDeclare(EXCHANGE_NAME,"fanout");
        //声明一个队列    临时
        /**
         * 生成一个临时队列、队列的名称是随机的
         * 当消费者断开与队列的连接的时候，队列主动删除
         */
        String queueName = channel.queueDeclare().getQueue();
        /**
         * 绑定交换机与队列
         * 1.队列名称
         * 2.交换机名称
         * 3.routingKey
         */
        channel.queueBind(queueName,EXCHANGE_NAME,"");
        System.out.println("ReceiveLogs01等待接收消息，把接收到的消息打印......");

        //接收消息
        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println("ReceiveLogs01控制台打印接收到的消息："+new String(message.getBody(), StandardCharsets.UTF_8));
        };

        /**
         * 消费者消费消息
         * 1.消费哪个队列
         * 2.是否自动应答
         * 3.接受消息回调
         * 4.取消消费回调
         */
        channel.basicConsume(queueName,true,deliverCallback,consumerTag->{});
    }
}
