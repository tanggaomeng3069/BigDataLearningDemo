package com.learning.nokerberos.case3;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Author: tanggaomeng
 * @Date: 2021/8/4 10:41
 * @Description:
 * @Version 1.0
 */
public class CuratorLockTest {

    public static void main(String[] args) {
        // 创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");
        // 创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire();
                    System.out.println("线程1 获取到锁");

                    lock1.acquire();
                    System.out.println("线程1 再次获取到锁");

                    Thread.sleep(5 * 1000);

                    lock1.release();
                    System.out.println("线程1 释放锁");

                    lock1.release();
                    System.out.println("线程1  再次释放锁");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.acquire();
                    System.out.println("线程2 获取到锁");

                    lock2.acquire();
                    System.out.println("线程2 再次获取到锁");

                    Thread.sleep(5 * 1000);

                    lock2.release();
                    System.out.println("线程2 释放锁");

                    lock2.release();
                    System.out.println("线程2  再次释放锁");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    private static CuratorFramework getCuratorFramework() {
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);

        CuratorFramework zkClient = CuratorFrameworkFactory.builder().connectString("manager.bigdata:2181,master.bigdata:2181,worker.bigdata:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy).build();

        // 启动客户端
        zkClient.start();

        System.out.println("zookeeper 启动成功");
        return zkClient;
    }
}
