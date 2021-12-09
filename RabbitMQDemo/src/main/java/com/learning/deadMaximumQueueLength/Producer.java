package com.learning.deadMaximumQueueLength;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/9 11:28
 * @Description:    设置队列最大长度
 * 0.如果有队列，先删除队列
 * 1.先执行 Consumer01 创建出队列，然后关闭
 * 2.启动 Producer生产数据，观察数据入：normal-queue，超过队列长度，最先入队列的数据转移到：dead-queue
 * 3.再启动 Consumer01，消费到数据最后队列长度保存的数据个数
 * 4.最后启动 Consumer02 从死信队列中消费最先入队列数据，超过长度被转移死信队列的数据
 *
 * @Version: 1.0
 */
public class Producer {
    private static final String NORMAL_EXCHANGE = "normal_exchange";

    public static void main(String[] args) {
        try (final Channel channel = RabbitMqUtils.getChannel()) {
            // 声明Exchange direct模式
            channel.exchangeDeclare(NORMAL_EXCHANGE, BuiltinExchangeType.DIRECT);
            // 该消息是用作演示队列个数限制
            for (int i = 0; i < 11; i++) {
                String message = "info: " + i;
                // 发送消息（发送到交换机，路由队列，其他参数，发送消息体）
                channel.basicPublish(NORMAL_EXCHANGE, "zhangsan", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("生产者发送消息：" + message);
            }

        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
