package com.fu.rabbitmq.a;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 生产者
 */
public class Producer {
    //队列名称
    public static  final String QUEUE_NAME = "hello";

    //发消息
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

        /**
         * 生成一个队列
         * 1.队列名称
         * 2.是否队列消息持久化
         * 3.是否一条信息只提供一个消费者消费
         * 4.是否自动删除
         * 5.其它参数
         */
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        //发消息
        String message = "hello world.";

        /**
         * 发送一个消息
         * 1.发送到哪个交换机
         * 2.路由的key 本次队列的名称
         * 3.其它参数信息
         * 4.发送消息的消息体
         */
        channel.basicPublish("",QUEUE_NAME,null,message.getBytes(StandardCharsets.UTF_8));
        System.out.println("发送消息完毕");
    }
}
