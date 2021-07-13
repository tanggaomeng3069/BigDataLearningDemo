package com.learning.xiaoniuxuetang.sparkstreaming;//package cn.edu360.xiaoniu.sparkstreaming;
//
//import org.I0Itec.zkclient.exception.ZkMarshallingError;
//import org.I0Itec.zkclient.serialize.ZkSerializer;
//
///**
// * Author: tanggaomeng
// * Date: 2020/9/23 14:37
// * Describe:
// */
//public class ZkSerializers implements ZkSerializer {
//
//    //序列化，数据--》byte[]
//    public byte[] serialize(Object o) throws ZkMarshallingError {
//        return String.valueOf(o).getBytes();
//    }
//    //反序列化，byte[]--->数据
//    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
//        return new String(bytes);
//    }
//
//}
