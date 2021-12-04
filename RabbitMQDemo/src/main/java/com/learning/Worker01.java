package com.learning;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import java.lang.String;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/4 10:45
 * @Description: 启动两个工作线程
 * @Version: 1.0
 */
public class Worker01 {
    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {
        final Channel channel = RabbitMqUtils.getChannel();
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            final String receiveMessage = new String(delivery.getBody());
            System.out.println("接收到消息：" + receiveMessage);
        };
        CancelCallback cancelCallback = (consumerTag) -> {
            System.out.println(consumerTag+"消费者取消消费接口回调逻辑");
        };
        System.out.println("C2消费者启动等待消费...");
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback);

    }

}
