package com.learning;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 14:05
 * @Description: 不公平分发消息方式：Worker041、Worker042，空闲发送消息
 * @Version: 1.0
 */
public class Task04 {
    private static final String TASK_QUEUE_NAME = "ack_queue_durable";

    public static void main(String[] args) {
        // 1.创建连接RabbitMQ channel
        try (final Channel channel = RabbitMqUtils.getChannel()) {
            // 不公平分发消息
            int prefetchCount = 1;
            channel.basicQos(prefetchCount);

            // 2.生成队列（队列名称，持久化，队列不共享，队列不自动删除，其他参数）
            // 消息持久化：需要把原先存在的队列删除或者重新创建一个新的队列，否则报错
            boolean durable = true;
            channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
            // 3.等待控制台输入
            final Scanner scanner = new Scanner(System.in);
            System.out.println("请输入信息: ");
            while (scanner.hasNext()) {
                final String message = scanner.nextLine();
                // 4.发送消息（发送到交换机，路由队列，其他参数，发送消息体）
                // 当durable为true的时候，持久化队列
                channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("生产者发送消息: " + message);
            }
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }


    }

}
