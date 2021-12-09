package com.learning.deadRejectedConsumption;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/9 13:43
 * @Description: 启动之后立即关闭该消费者，模拟其接收不到消息
 * @Version: 1.0
 */
public class Consumer01 {
    // 普通交换机名称
    private static final String NORMAL_EXCHANGE = "normal_exchange";
    // 死信交换机名称
    private static final String DEAD_EXCHANGE = "dead_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        final Channel channel = RabbitMqUtils.getChannel();

        // 声明死信交换机、普通交换机，类型为direct
        channel.exchangeDeclare(NORMAL_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(DEAD_EXCHANGE, BuiltinExchangeType.DIRECT);

        // 声明死信队列
        String deadQueue = "dead-queue";
        channel.queueDeclare(deadQueue, false, false, false, null);
        // 死信队列绑定死信交换机，设置routingkey
        channel.queueBind(deadQueue, DEAD_EXCHANGE, "lisi");

        // 正常队列绑定死信队列
        // 注意：如果修改了参数，需要先删除队列，再重新生成
        final HashMap<String, Object> params = new HashMap<>();
        // 正常队列设置死信交换机 参数 key 是固定值
        params.put("x-dead-letter-exchange", DEAD_EXCHANGE);
        // 正常队列设置死信 routing-key 参数 key 是固定值
        params.put("x-dead-letter-routing-key", "lisi");

        // 声明正常队列
        String normalQueue = "normal-queue";
        channel.queueDeclare(normalQueue, false, false, false, params);
        // 正常队列绑定正常交换机，设置routingkey
        channel.queueBind(normalQueue, NORMAL_EXCHANGE, "zhangsan");

        System.out.println("等待接收消息...");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            final String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (message.equals("info5")) {
                System.out.println("Consumer01 接收到消息：" + message + "并拒绝签收该消息");
                //requeue 设置为 false 代表拒绝重新入队 该队列如果配置了死信交换机将发送到死信队列中
                channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
            } else {
                System.out.println("Consumer01 接收到消息：" + message);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        // 手动应答
        boolean autoAck = false;
        channel.basicConsume(normalQueue, autoAck, deliverCallback, consumerTag -> {
        });
    }

}
