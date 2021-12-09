package com.learning.deadRejectedConsumption;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/9 13:59
 * @Description:
 * @Version: 1.0
 */
public class Consumer02 {
    private static final String DEAD_EXCHANGE = "dead_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        // 获取连接
        Channel channel = RabbitMqUtils.getChannel();
        // 声明死信交换机
        channel.exchangeDeclare(DEAD_EXCHANGE, BuiltinExchangeType.DIRECT);
        // 声明死信队列
        String deadQueue = "dead-queue";
        channel.queueDeclare(deadQueue, false, false, false, null);
        // 死信队列绑定死信交换机
        channel.queueBind(deadQueue, DEAD_EXCHANGE, "lisi");

        System.out.println("等待接收死信队列消息.....");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Consumer02 接收死信队列的消息：" + message);
        };

        channel.basicConsume(deadQueue, true, deliverCallback, consumerTag -> {
        });

    }
}
