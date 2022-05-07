package com.fu.rabbitmq.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitmqUtil {
    public static Channel getChannel() throws IOException, TimeoutException {
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
        return connection.createChannel();
    }
}
