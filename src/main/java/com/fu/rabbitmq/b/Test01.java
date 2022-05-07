package com.fu.rabbitmq.b;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 生产者
 */
public class Test01 {
    //队列名称
    public static  final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {
        //发送大量消息
        Channel channel = RabbitmqUtil.getChannel();

        /**
         * 生成一个队列
         * 1.队列名称
         * 2.是否队列消息持久化
         * 3.是否一条信息只提供一个消费者消费
         * 4.是否自动删除
         * 5.其它参数
         */
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);

        //从控制台接收消息
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()){
            String message = scanner.next();
            /**
             * 发送一个消息
             * 1.发送到哪个交换机
             * 2.路由的key 本次队列的名称
             * 3.其它参数信息
             * 4.发送消息的消息体
             */
            channel.basicPublish("",QUEUE_NAME,null,message.getBytes(StandardCharsets.UTF_8));
            System.out.println("发送消息完成："+message);
        }
    }
}
