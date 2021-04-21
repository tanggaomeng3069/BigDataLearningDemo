package com.learning.nokerberos.rpc;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/21 17:03
 * @Description:
 * @Version 1.0
 */
public interface RPCProtocol {
    long versionID = 666;

    void mkdirs(String path);
}
