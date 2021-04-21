package com.learning.nokerberos.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/21 17:09
 * @Description:
 * @Version 1.0
 */
public class HDFSClient {

    public static void main(String[] args) throws IOException {

        // 获取客户端对象
        RPCProtocol client = RPC.getProxy(RPCProtocol.class, RPCProtocol.versionID, new InetSocketAddress("localhost"
                , 8888), new Configuration());

        System.out.println("客户端开始工作啦。。。");

        client.mkdirs("/input");

    }
}
