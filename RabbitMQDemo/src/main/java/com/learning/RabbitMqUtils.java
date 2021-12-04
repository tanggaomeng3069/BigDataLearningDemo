package com.learning;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/4 10:30
 * @Description: 抽取工具类
 * @Version: 1.0
 */
public class RabbitMqUtils {
    // 得到一个连接的channel
    public static Channel getChannel() throws IOException, TimeoutException {
        // 创建一个连接工厂
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("192.168.120.15");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin123");
        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        return channel;
    }
}
