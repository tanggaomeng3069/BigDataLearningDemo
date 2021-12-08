package com.learning;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 14:05
 * @Description: 分发消息采用轮询的方式：Worker021、Worker022，轮询发送消息
 * @Version: 1.0
 */
public class Task02 {
    private static final String TASK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) {
        // 1.创建连接RabbitMQ channel
        try(final Channel channel = RabbitMqUtils.getChannel()){
            // 2.生成队列（队列名称，不持久化，队列不共享，队列不自动删除，其他参数）
            channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
            // 3.等待控制台输入
            final Scanner scanner = new Scanner(System.in);
            System.out.println("请输入信息: ");
            while (scanner.hasNext()){
                final String message = scanner.nextLine();
                // 4.发送消息（发送到交换机，路由队列，其他参数，发送消息体）
                channel.basicPublish("", TASK_QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("生产者发送消息: " + message);
            }
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }


    }

}
