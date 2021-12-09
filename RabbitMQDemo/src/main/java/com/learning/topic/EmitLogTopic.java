package com.learning.topic;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/9 10:51
 * @Description:
 * @Version: 1.0
 */
public class EmitLogTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) {
        try (final Channel channel = RabbitMqUtils.getChannel()) {
            // 声明交换机类型
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            /**
             * Q1 ---> 绑定的是
             *          中间带有orange带3个单词的字符（*.orange.*）
             * Q2 ---> 绑定的是
             *          最后一个单词是rabbit的3个单词（*.*.rabbit）
             *          第一个单词是lazy的多个单词（lazy.#）
             */
            final HashMap<String, String> bindingKeyMap = new HashMap<>();
            bindingKeyMap.put("quick.orange.rabbit", "被队列 Q1Q2 接收到");
            bindingKeyMap.put("lazy.orange.elephant", "被队列 Q1Q2 接收到");
            bindingKeyMap.put("quick.orange.fox", "被队列 Q1 接收到");
            bindingKeyMap.put("lazy.brown.fox", "被队列 Q2 接收到");
            bindingKeyMap.put("lazy.pink.rabbit", "虽然满足两个绑定但只被队列 Q2 接收一次");
            bindingKeyMap.put("quick.brown.fox", "不匹配任何绑定不会被任何队列接收到会被丢弃");
            bindingKeyMap.put("quick.orange.male.rabbit", "是四个单词不匹配任何绑定会被丢弃");
            bindingKeyMap.put("lazy.orange.male.rabbit", "是四个单词但匹配 Q2");

            for (Map.Entry<String, String> bindingKeyEntry : bindingKeyMap.entrySet()) {
                final String bindingKey = bindingKeyEntry.getKey();
                final String message = bindingKeyEntry.getValue();
                // 发送消息
                channel.basicPublish(EXCHANGE_NAME, bindingKey, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("生产者发送消息：" + message);
            }

        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
