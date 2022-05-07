package com.fu.rabbitmq.e;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 交换机：生产者（发布、订阅模式相当于广播）
 * 一个交换机发送给多个消费者，扇出fanout（routing key是一样的）
 */
public class EmitLog {

    //交换机名称
    public static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();
        //声明一个交换机
        channel.exchangeDeclare(EXCHANGE_NAME,"fanout");

        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()){
            String message = scanner.next();
            channel.basicPublish(EXCHANGE_NAME,"",null,message.getBytes(StandardCharsets.UTF_8));
            System.out.println("生产者发出消息："+message);
        }

    }

}
