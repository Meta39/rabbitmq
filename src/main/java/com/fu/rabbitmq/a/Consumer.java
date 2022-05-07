package com.fu.rabbitmq.a;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者
 */
public class Consumer {
    //队列名称
    public static final String QUEUE_NAME = "hello";

    //接收消息
    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //工厂IP  连接RabbitMQ得队列
        factory.setHost("localhost");
        //账号
        factory.setUsername("guest");
        //密码
        factory.setPassword("guest");

        //创建连接
        Connection connection = factory.newConnection();
        //获取信道
        Channel channel = connection.createChannel();

        //声明    接受消息
        DeliverCallback deliverCallback = (consumerTag,message) -> {
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
        channel.basicConsume(QUEUE_NAME,true,deliverCallback,cancelCallback);
    }
}
