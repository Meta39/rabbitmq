package com.fu.rabbitmq.f;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 交换机：生产者（直接换机）
 * 一个交换机发送给多个消费者，direct（routing key是不一样的）
 * 交换机名称一样，routing_key不一样。
 */
public class DirectLogs {
    //交换机名称
    public static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();

        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()){
            String message = scanner.next();
            String routingKey = "info";
            channel.basicPublish(EXCHANGE_NAME,routingKey,null,message.getBytes(StandardCharsets.UTF_8));
            System.out.println("生产者发出消息："+message);
        }

    }
}
