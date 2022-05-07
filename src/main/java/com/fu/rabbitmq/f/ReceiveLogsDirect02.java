package com.fu.rabbitmq.f;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 交换机：消费者
 * direct直接交换机
 */
public class ReceiveLogsDirect02 {

    public static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();
        //声明一个交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //声明一个队列
        channel.queueDeclare("disk",false,false,false,null);
        /**
         * 绑定交换机与队列
         * 1.队列名称
         * 2.交换机名称
         * 3.routingKey
         */
        channel.queueBind("disk",EXCHANGE_NAME,"error");

        //接收消息
        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println("ReceiveLogsDirect01控制台打印接收到的消息："+new String(message.getBody(), StandardCharsets.UTF_8));
        };

        /**
         * 消费者消费消息
         * 1.消费哪个队列
         * 2.是否自动应答
         * 3.接受消息回调
         * 4.取消消费回调
         */
        channel.basicConsume("disk",true,deliverCallback,consumerTag->{});
    }
}
