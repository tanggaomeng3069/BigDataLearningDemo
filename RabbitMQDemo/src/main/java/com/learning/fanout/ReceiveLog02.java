package com.learning.fanout;

import com.learning.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 19:19
 * @Description:
 * @Version: 1.0
 */
public class ReceiveLog02 {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        /**
         * 生成一个临时的队列 队列的名称是随机的
         * 当消费者断开和该队列的连接时 队列自动删除
         */
        String queueName = channel.queueDeclare().getQueue();
        //把该临时队列绑定我们的 exchange 其中 routingkey(也称之为 binding key)为空字符串
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        System.out.println("等待接收消息，把接收到的消息写到文件...");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            File file = new File("D:\\code_data\\Data\\rabbitmq_info.txt");
            // 重写文件
            // FileUtils.writeStringToFile(file, message, "UTF-8");
            // 换行追加写入
            FileUtils.writeStringToFile(file, message + "\n", "UTF-8", true);
            System.out.println("数据写入文件成功");
        };
        // 消费数据（消费队列，消费自动应答，消费者未成功消费的回调， 取消消费回调）
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });

    }

}
