package com.learning.direct;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/9 9:11
 * @Description: Exchange -> Direct 直接模式
 * @Version: 1.0
 */
public class ReceiveLogsDirect01 {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        // 获取连接
        final Channel channel = RabbitMqUtils.getChannel();
        // 声明交换机及类型
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        // 队列名称
        String queueName = "disk";
        // 生成队列（队列名称，不持久化，不共享，不自动删除，其他参数）
        channel.queueDeclare(queueName, false, false, false, null);
        // 交换机和队列绑（队列名称，交换机名称，routingkey）
        channel.queueBind(queueName, EXCHANGE_NAME, "error");
        System.out.println("等待接收消息...");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            message = "接收绑定键：" + delivery.getEnvelope().getRoutingKey() + "，消息：" + message;
            File file = new File("D:\\code_data\\Data\\rabbitmq_info.txt");
            // 重写文件
            //FileUtils.writeStringToFile(file, message, "UTF-8");
            // 换行追加写入
            FileUtils.writeStringToFile(file, message + "\n", "UTF-8", true);
            System.out.println("错误日志已经接收");
        };

        // 消费数据
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
