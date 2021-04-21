package com.learning.nokerberos.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/21 17:04
 * @Description:
 * @Version 1.0
 * 实现通讯接口
 */
public class NNServer implements RPCProtocol {

    public static void main(String[] args) throws IOException {
        // 启动服务
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer())
                .build();

        System.out.println("服务器开始工作啦。。。");
        server.start();

    }

    @Override
    public void mkdirs(String path) {
        System.out.println("服务器接收到了客户端请求： " + path);
    }
}
