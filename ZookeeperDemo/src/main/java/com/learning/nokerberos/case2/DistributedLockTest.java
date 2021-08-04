package com.learning.nokerberos.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/8/4 10:16
 * @Description:
 * @Version 1.0
 */
public class DistributedLockTest {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        final DistributedLock lock1 = new DistributedLock();
        final DistributedLock lock2 = new DistributedLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.zkLock();
                    System.out.println("线程1 启动，获取到锁");
                    Thread.sleep(5 * 1000);
                    lock1.unZkLock();
                    System.out.println("线程1 释放锁");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.zkLock();
                    System.out.println("线程2 启动，获取到锁");
                    Thread.sleep(5 * 1000);
                    lock2.unZkLock();
                    System.out.println("线程2 释放锁");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }
}
