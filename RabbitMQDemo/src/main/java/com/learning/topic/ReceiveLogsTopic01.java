package com.learning.topic;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/9 11:07
 * @Description:
 * @Version: 1.0
 */
public class ReceiveLogsTopic01 {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        final Channel channel = RabbitMqUtils.getChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        // 声明Q1队列与绑定关系
        String queueName = "Q1";
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queueBind(queueName, EXCHANGE_NAME, "*.orange.*");
        System.out.println("Q1 等待接收消息...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            final String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("接收队列：" + queueName + " - 绑定键：" + delivery.getEnvelope().getRoutingKey() + " - 消息：" + message);
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
