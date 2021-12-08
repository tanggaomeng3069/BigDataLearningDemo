package com.learning;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 15:50
 * @Description:
 * @Version: 1.0
 */
public class ProducerPublish {
    private static final Integer MESSAGE_COUNT = 1000;

    /**
     * 单个确认发布：单个消息，同步确认发布，阻塞的，发布速度特别慢，每秒不超过百条发布消息的吞吐量
     */
    public static void publishMessageIndividually() {
        try (final Channel channel = RabbitMqUtils.getChannel()) {
            // 使用UUID当作队列名称
            final String queueName = UUID.randomUUID().toString();
            // 创建队列（队列名称，是否持久化，队列不共享，队列不自动删除，其他参数）
            channel.queueDeclare(queueName, false, false, false, null);
            // 开启发布确认
            channel.confirmSelect();
            final long begin = System.currentTimeMillis();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String message = i + "";
                // 发送消息（交换机，队列，其他参数，消息体）
                channel.basicPublish("", queueName, null, message.getBytes());
                // 服务端返回false或者超时时间内未返回，生产者可以消息重发
                boolean flag = channel.waitForConfirms();
                if (flag) {
                    System.out.println("消息发送成功");
                }
            }
            final long end = System.currentTimeMillis();
            System.out.println("发布：" + MESSAGE_COUNT + "个单独确认消息，耗时：" + (end - begin) + "ms");

        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量确认发布：一批消息，同步确认发布，阻塞的，发布速度比单个确认发布快
     */
    public static void publishMessageBatch() {
        try (final Channel channel = RabbitMqUtils.getChannel()) {
            // 使用UUID当作队列名称
            final String queueName = UUID.randomUUID().toString();
            // 创建队列（队列名称，是否持久化，队列不共享，队列不自动删除，其他参数）
            channel.queueDeclare(queueName, false, false, false, null);
            // 开启发布确认
            channel.confirmSelect();
            // 批量确认消息大小
            int batchSize = 100;
            // 未确认消息个数
            int outstandingMessageCount = 0;
            final long begin = System.currentTimeMillis();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String message = i + "";
                // 发送消息（交换机，队列，其他参数，消息体）
                channel.basicPublish("", queueName, null, message.getBytes());
                outstandingMessageCount += 1;
                if (outstandingMessageCount == batchSize) {
                    channel.waitForConfirms();
                    outstandingMessageCount = 0;
                }
            }
            // 为了确保还有剩余 < batchSize 没有确认消息，再次确认
            if (outstandingMessageCount > 0) {
                channel.waitForConfirms();
            }
            final long end = System.currentTimeMillis();
            System.out.println("发布：" + MESSAGE_COUNT + "个批量确认消息，耗时：" + (end - begin) + "ms");

        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步确认发布：性价比最高
     */
    public static void publishMessageAsync() {
        try (final Channel channel = RabbitMqUtils.getChannel()) {
            // 使用UUID当作队列名称
            final String queueName = UUID.randomUUID().toString();
            // 创建队列（队列名称，是否持久化，队列不共享，队列不自动删除，其他参数）
            channel.queueDeclare(queueName, false, false, false, null);
            // 开启发布确认
            channel.confirmSelect();
            /**
             * 线程安全有序的一个哈希表，适用于高并发的去情况
             * 1.轻松的将序号与消息进行关联
             * 2.轻松批量删除条目，只要给到序列号
             * 3.支持并发访问
             */
            final ConcurrentSkipListMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            /**
             * 确认收到消息的一个回调
             * 1.消息序列号
             * 2.true可以确认小于等于当前序列号的消息
             *   false确认当前序列号消息
             */
            ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
                if (multiple) {
                    // 返回的是小于等于当前序列号的未确认的消息，是一个map
                    final ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                    // 清除该部分未确认消息
                    confirmed.clear();
                } else {
                    // 只清除当前序列号的消息
                    outstandingConfirms.remove(sequenceNumber);
                }
            };

            ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
                String message = outstandingConfirms.get(sequenceNumber);
                System.out.println("发布的消息：" + message + "未被确认，序列号：" + sequenceNumber);
            };

            /**
             * 添加一个异步确认的监听器
             * 1.确认收到消息的回调
             * 2.未收到消息的回调
             */
            channel.addConfirmListener(ackCallback, null);
            final long begin = System.currentTimeMillis();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String message = "消息" + i;
                /**
                 * channel.getNextPublishSeqNo()获取下一个消息的序列号
                 * 通过序列号与消息进行一个关联
                 * 全部都是未确认的消息体
                 */
                outstandingConfirms.put(channel.getNextPublishSeqNo(), message);
                // 发送消息（发送到交换机，路由队列，其他参数，发送消息体）
                channel.basicPublish("", queueName, null, message.getBytes());
            }
            final long end = System.currentTimeMillis();
            System.out.println("发布：" + MESSAGE_COUNT + "个异步确认消息，耗时：" + (end - begin) + "ms");

        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 消息数量设置为：1000，查看发布时间
        // 单独确认：发布：1000个单独确认消息，耗时：600ms
        publishMessageIndividually();
        // 批量确认：发布：1000个批量确认消息，耗时：109ms
        publishMessageBatch();
        // 异步确认：发布：1000个异步确认消息，耗时：27ms
        publishMessageAsync();
    }

}
