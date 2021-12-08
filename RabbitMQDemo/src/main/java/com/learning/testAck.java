package com.learning;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 13:49
 * @Description:
 * @Version: 1.0
 */
public class testAck {
    private static final String TASK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();
        channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
        System.out.println("C1等待接收消息...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            final String receiveMessage = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // SleepUtils.sleep(10);
            System.out.println("接收到消息：" + receiveMessage);
            // 1.标记消息Tag
            // 2.false代表只应答接收到的那个传递的消息，
            // true为应答所有消息包括传递过来的消息
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        // 手动应答
        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });

    }

}
