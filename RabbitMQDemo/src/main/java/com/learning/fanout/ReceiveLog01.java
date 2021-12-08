package com.learning.fanout;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 19:11
 * @Description:
 * @Version: 1.0
 */
public class ReceiveLog01 {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        final Channel channel = RabbitMqUtils.getChannel();
        // 声明交换机和类型
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        /**
         * 生成一个临时队列 队列的名称是随机的
         * 当消费者断开和该队列的连接时 队列自动删除
         */
        final String queueName = channel.queueDeclare().getQueue();
        // 把该临时队列绑定到Exchange，其中routingkey（也称为binding key）为空字符串
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        System.out.println("等待接收消息，把接收到的消息打印在屏幕...");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("控制台打印接收到的消息：" + message);
        };
        // 消费数据（消费队列，消费自动应答，消费者未成功消费的回调， 取消消费回调）
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });


    }

}
