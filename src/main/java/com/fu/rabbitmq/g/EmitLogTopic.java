package com.fu.rabbitmq.g;

import com.fu.rabbitmq.util.RabbitmqUtil;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Topic主题交互机：生产者
 */
public class EmitLogTopic {

    //交互机名称
    public static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitmqUtil.getChannel();

        Map<String,String> bindingKeyMap = new HashMap<>();
        bindingKeyMap.put("quick.orange.rabbit","Q1Q2接收到");
        bindingKeyMap.put("lazy.orange.elephant","Q1Q2接收到");
        bindingKeyMap.put("quick.orange.fox","Q1接收到");
        bindingKeyMap.put("lazy.brown.fox","Q2接收到");
        bindingKeyMap.put("lazy.pink.rabbit","满足2个绑定，但只能Q2接收到，因为是同一个队列不同routing_key");
        bindingKeyMap.put("quick.brown.fox","不匹配任何绑定会被抛弃");
        bindingKeyMap.put("quick.orange.male.rabbit","四个单词不匹配任何绑定会被抛弃");
        bindingKeyMap.put("lazy.orange.male.rabbit","四个单词匹配Q2绑定");

        for (Map.Entry<String, String> bindingKeyEntry : bindingKeyMap.entrySet()) {
            channel.basicPublish(EXCHANGE_NAME,bindingKeyEntry.getKey(),null,bindingKeyEntry.getValue().getBytes(StandardCharsets.UTF_8));
            System.out.println("生产者发出消息："+bindingKeyEntry.getValue());
        }
    }
}
