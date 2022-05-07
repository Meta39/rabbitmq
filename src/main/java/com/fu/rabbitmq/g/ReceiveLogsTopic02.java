package com.fu.rabbitmq.g;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Topic主题交互机02：消费者
 * 星号*代替一个单词
 * 井号#代替零个或多个单词
 */
public class ReceiveLogsTopic02 {
    //交互机名称
    public static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();

        //声明交互机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        //声明队列
        String queueName = "Q2";
        channel.queueDeclare(queueName,false,false,false,null);
        /**
         * 星号*代替一个单词
         * 井号#代替零个或多个单词
         */
        channel.queueBind(queueName,EXCHANGE_NAME,"*.*.rabbit");
        channel.queueBind(queueName,EXCHANGE_NAME,"lazy.#");
        System.out.println("等待接收消息......");

        //接收消息
        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println("ReceiveLogsTopic01控制台打印接收到队列："+queueName+"  绑定键："+message.getEnvelope().getRoutingKey()+"的消息："+new String(message.getBody(), StandardCharsets.UTF_8));
        };

        /**
         * 消费者消费消息
         * 1.消费哪个队列
         * 2.是否自动应答
         * 3.接受消息回调
         * 4.取消消费回调
         */
        channel.basicConsume(queueName,true,deliverCallback,consumerTag -> {});

    }
}
