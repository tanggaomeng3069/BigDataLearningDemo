package com.learning.nokerberos.case1;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/8/3 16:41
 * @Description:
 * @Version 1.0
 */
public class DistributeServer {

    private ZooKeeper zkClient;
    private String connectString = "manager.bigdata:2181,master.bigdata:2181,worker.bigdata:2181";
    private int sessionTimeout = 2000;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeServer server = new DistributeServer();
        // 1.获取zk链接
        server.getConnect();
        // 2.注册服务器到zk集群
        server.regist(args[0]);
        // 3.启动业务逻辑（睡觉）
        server.business();

    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {
        String created = zkClient.create("/servers/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online");
    }

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });

    }
}
