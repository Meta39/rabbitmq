package com.fu.rabbitmq.c;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 生产者：手动应答
 * 队列持久化
 * 消息持久化
 */
public class Task2 {
    //队列名称
    public static  final String TASK_QUEUE_NAME = "ack_queue";


    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();

        //声明队列
        /**
         * 生成一个队列
         * 1.队列名称
         * 2.是否队列消息持久化
         * 3.是否一条信息只提供一个消费者消费
         * 4.是否自动删除
         * 5.其它参数
         */
        //持久化队列
        boolean durable = true;
        channel.queueDeclare(TASK_QUEUE_NAME,durable,false,false,null);
        //从控制台输入消息
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()){
            String message = scanner.next();
            /**
             * 发送一个消息
             * 1.发送到哪个交换机
             * 2.路由的key 本次队列的名称
             * 3.其它参数信息 如：消息持久化MessageProperties.PERSISTENT_TEXT_PLAIN
             * 4.发送消息的消息体
             */
            //生产者发生消息持久化    MessageProperties.PERSISTENT_TEXT_PLAIN
            channel.basicPublish("",TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes(StandardCharsets.UTF_8));
            System.out.println("生产者发出消息："+message);
        }
    }
}
