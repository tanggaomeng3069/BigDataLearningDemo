package com.learning;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 14:18
 * @Description:
 * @Version: 1.0
 */
public class Worker042 {
    private static final String TASK_QUEUE_NAME = "ack_queue_durable";

    public static void main(String[] args) throws IOException, TimeoutException {
        // 1.获取RabbitMQ连接channel
        final Channel channel = RabbitMqUtils.getChannel();
        // 不公平分发消息
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        System.out.println("C42等待接收消息处理时间较短...");
        // 2.消息消费的时候如何处理
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            SleepUtils.sleep(30);
            System.out.println("接收到消息: " + message);
            /**
             * 3.消息标记 Tag
             * 是否批量应答未应答消息:
             *   false代表只应答接收到的那个传递的消息，
             *   true为应答所有消息包括传递过来的消息
             */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        // 取消消费的一个回调接口 如在消费的时候队列被删除掉了
        CancelCallback cancelCallback = (consumerTag) -> {
            System.out.println(consumerTag + "消费者取消消费接口回调逻辑");
        };
        // 4.采用手动应答 - 消费成功之后是否要自动应答：true-代表自动应答；false-手动应答
        boolean autoAck = false;
        // 5.消费者消费消息（消费队列，消费手动应答，消费者未成功消费的回调， 取消消费回调）
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, cancelCallback);
    }
}
