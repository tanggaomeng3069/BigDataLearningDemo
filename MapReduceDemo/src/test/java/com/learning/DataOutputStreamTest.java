package com.learning;

import java.io.*;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 10:07
 * @Description:
 * @Version 1.0
 */
public class DataOutputStreamTest {

    public static void main(String[] args) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream("inputData/a.dat"));
        dos.write("中国，你好！".getBytes("utf-8"));
        dos.close();

        DataOutputStream dos2 = new DataOutputStream(new FileOutputStream("inputData/b.dat"));
        dos2.writeUTF("中国，你好！");
        dos2.close();

    }
}
