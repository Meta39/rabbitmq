package com.fu.rabbitmq.d;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

/**
 * 发布确认模式
 * 1.单个确认（简单）       178ms
 * 2.批量确认（简单）       30ms
 * 3.异步批量确认（复杂）     9ms
 * 建议异步单个确认：
 * 一、记录所有要发送的消息  消息的总和
 * 二、删除掉已经确认的消息  剩下的就是未确认的消息
 * 三、打印一下未确认的消息有哪些
 */
public class ConfirmMessage {

    //批量发消息的个数
    public static final int MESSAGE_COUNT = 1000;

    public static void main(String[] args) throws InterruptedException, TimeoutException, IOException {
        //1.单个确认
        ConfirmMessage.publishMessageOne();
        //2.批量确认
        ConfirmMessage.publishMessageBatch();
        //3.异步批量确认
        ConfirmMessage.publishMessageAsync();
    }

    //单个确认
    public static void publishMessageOne() throws IOException, TimeoutException, InterruptedException {
        Channel channel = RabbitmqUtil.getChannel();
        //队列的声明
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName,false,false,false,null);

        //开启发布确认
        channel.confirmSelect();

        //开始时间
        long begin = System.currentTimeMillis();

        //批量发消息
        for (int i = 0;i<MESSAGE_COUNT;i++){
            String message = i+"";
            channel.basicPublish("",queueName,null,message.getBytes(StandardCharsets.UTF_8));
            //单个消息就马上进行发布确认
            boolean flag = channel.waitForConfirms();
            if (flag) System.out.println("消息发送成功");
        }
        //结束时间
        long end = System.currentTimeMillis();
        System.out.println("发布"+MESSAGE_COUNT+"条单个确认消息耗时："+(end - begin)+"毫秒");
    }

    //批量确认
    public static void publishMessageBatch() throws IOException, TimeoutException, InterruptedException {
        Channel channel = RabbitmqUtil.getChannel();
        //队列的声明
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName,false,false,false,null);

        //开启发布确认
        channel.confirmSelect();

        //开始时间
        long begin = System.currentTimeMillis();

        //批量确认消息大小
        int batchSize = 100;

        //批量发消息 批量确认
        for (int i = 0;i<MESSAGE_COUNT;i++){
            String message = i+"";
            channel.basicPublish("",queueName,null,message.getBytes(StandardCharsets.UTF_8));
            //判断达到100条消息时，批量确认一次
            if (i%batchSize == 0){
                //发布确认
                channel.waitForConfirms();
            }
        }
        //结束时间
        long end = System.currentTimeMillis();
        System.out.println("发布"+MESSAGE_COUNT+"条批量确认消息耗时："+(end - begin)+"毫秒");
    }

    //异步批量确认
    public static void publishMessageAsync() throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();
        //队列的声明
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName,false,false,false,null);

        //开启发布确认
        channel.confirmSelect();

        /**
         * 线程安全有序的哈希表   适用于高并发的情况下
         * 1.轻松的将序列号与消息进行关联
         * 2.轻松批量删除条目   只要给到序列号
         * 3.支持高并发（多线程）
         */
        ConcurrentSkipListMap<Long,String> outstandingConfirms = new ConcurrentSkipListMap<>();


        //开始时间
        long begin = System.currentTimeMillis();

        //消息确认成功    回调函数
        /**
         * 1.消息的标记
         * 2.是否为批量确认
         */
        ConfirmCallback ackCallback = (deliveryTag,multiple)->{
            //multiple是否批量
            if (multiple){
                //批量确认
                //二、删除掉已经确认的消息  剩下的就是未确认的消息
                ConcurrentNavigableMap<Long,String> confirmed = outstandingConfirms.headMap(deliveryTag);
                confirmed.clear();
            }else {
                //单个确认
                outstandingConfirms.remove(deliveryTag);
            }
            System.out.println("确认的消息："+deliveryTag);
        };
        //消息确认失败    回调函数
        /**
         * 1.消息的标记
         * 2.是否为批量确认
         */
        ConfirmCallback nackCallback = (deliveryTag,multiple)->{
            //三、打印一下未确认的消息有哪些
            String message = outstandingConfirms.get(deliveryTag);
            System.out.println("未确认的消息是："+message+"::::未确认的消息tag："+deliveryTag);
        };
        //准备消息的监听器  监听消息成功还是失败
        /**
         * 1.监听成功消息
         * 2.监听失败消息
         */
        channel.addConfirmListener(ackCallback,nackCallback);//异步通知

        for (int i = 0;i<MESSAGE_COUNT;i++){
            String message = i+"";
            channel.basicPublish("",queueName,null,message.getBytes(StandardCharsets.UTF_8));
            //一、记录所有要发送的消息  消息的总和
            outstandingConfirms.put(channel.getNextPublishSeqNo(),message);
        }

        //结束时间
        long end = System.currentTimeMillis();
        System.out.println("发布"+MESSAGE_COUNT+"条异步批量确认消息耗时："+(end - begin)+"毫秒");
    }
}
