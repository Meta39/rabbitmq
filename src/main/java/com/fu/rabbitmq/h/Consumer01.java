package com.fu.rabbitmq.h;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

/**
 * 死信队列：成为条件——》消息被拒绝、消息TTL过期（时间）、队列达到最大长度
 * 消费者1
 */
public class Consumer01 {

    //普通交互机名称
    public static final String NORMAL_EXCHANGE = "normal_exchange";
    //死信交换机名称
    public static final String DEAD_EXCHANGE = "dead_exchange";

    //普通队列名称
    public static final String NORMAL_QUEUE = "normal_queue";
    ///死信队列名称
    public static final String DEAD_QUEUE = "dead_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();
        //声明死信和普通交换机    类型为direct
        channel.exchangeDeclare(NORMAL_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(DEAD_EXCHANGE, BuiltinExchangeType.DIRECT);

        //声明普通队列
        HashMap<String, Object> arguments = new HashMap<>();
        //过期时间  10s=10000ms 一般由生产者发送
        //arguments.put("x-message-ttl",10000);
        //正常队列设置死信交换机
        arguments.put("x-dead-letter-exchange",DEAD_EXCHANGE);
        //设置死信RoutingKey
        arguments.put("x-dead-letter-routing-key","lisi");

        //*** 设置正常队列的长度的限制
        //**** arguments.put("x-max-length",6);

        channel.queueDeclare(NORMAL_QUEUE,false,false,false,arguments);

        //////////////////////////////////////////////////////
        //声明死信队列
        channel.queueDeclare(DEAD_QUEUE,false,false,false,null);

        //绑定普通队列与普通交换机
        channel.queueBind(NORMAL_QUEUE,NORMAL_EXCHANGE,"zhangsan");
        //绑定死信队列与死信交换机
        channel.queueBind(DEAD_QUEUE,DEAD_EXCHANGE,"lisi");
        System.out.println("等待接收消息.....");

        //开启手动应答
        channel.basicConsume(NORMAL_QUEUE,false, (consuumerTag,message) -> {
            String msg = new String(message.getBody(), StandardCharsets.UTF_8);
            if ("我是第5条消息".equals(msg)){
                System.out.println("Consumer01接收的消息是："+msg+"！！！此消息被拒绝进入到死信队列！");
                //不放回队列
                channel.basicReject(message.getEnvelope().getDeliveryTag(),false);
            }else {
                System.out.println("Consumer01接收的消息是："+msg);
                channel.basicAck(message.getEnvelope().getDeliveryTag(),false);
            }
        },consumerTag ->{});
    }
}
