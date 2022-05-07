package com.fu.rabbitmq.c;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 消费者：消费消息更慢的
 * Work03和Work04同时设置不公平应答prefetchCount =1;性能更好的处理更多消息
 * Work03和Work04同时设置prefetchCount>1，数字大的处理更多消息。
 */
public class Work04 {
    //队列名称
    public static  final String TASK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();

        System.out.println("C2处理信息较慢");

        //声明    接受消息
        DeliverCallback deliverCallback = (consumerTag, message) -> {
            //模拟处理过程漫长
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("接收到的消息："+new String(message.getBody(), StandardCharsets.UTF_8));
            //手动应答
            /**
             * 1.消息的标记 tag
             * 2.是否批量应答
             */
            channel.basicAck(message.getEnvelope().getDeliveryTag(),false);
        };

        //取消消息时的回调
        CancelCallback cancelCallback = consumerTag ->{
            System.out.println("消息消费取消消费接回调逻辑");
        };

        /**
         * 消费者消费消息
         * 1.消费哪个队列
         * 2.是否自动应答
         * 3.接受消息回调
         * 4.取消消费回调
         */
        //设置不公平分发
        int prefetchCount =1;
        channel.basicQos(prefetchCount);
        //采用手动应答
        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME,autoAck,deliverCallback,cancelCallback);
    }
}
