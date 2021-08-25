package com.learning.nokerberos.hdfsIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/8/25 9:49
 * @Description:
 * @Version: 1.0
 */
public class hdfsClient3 {
    private FileSystem fs;
    private Configuration configuration;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群NN地址
        URI uri = new URI("hdfs://manager.bigdata:8020");
        // 创建一个配置文件
        configuration = new Configuration();
        configuration.set("dfs.replication", "3");

        // 设置访问用户
        String user = "zhengzhou";

        // 获取客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 关闭资源
        fs.close();
    }

    /**
     *  把本地D盘上的 test1.txt 文件上传到HDFS根目录并重命名
     */
    @Test
    public void putFileToHDFS() throws IOException {
        // 创建输入流
        FileInputStream fileInputStream = new FileInputStream(new File("D:\\code_data\\hadoop\\test1.txt"));
        // 获取输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/test2.txt"));

        // 流对流拷贝
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, configuration);

        // 关闭资源
        IOUtils.closeStream(fsDataOutputStream);
        IOUtils.closeStream(fileInputStream);
    }

    /**
     * 从HDFS上下载test2.txt文件到d盘，并重命名
     */
    @Test
    public void getFileFromHDFS() throws IOException {
        // 获取输入流
        FSDataInputStream fsDataInputStream = fs.open(new Path("/test2.txt"));
        // 获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\code_data\\hadoop\\test3.txt"));

        // 流对流的拷贝
        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, configuration);

        // 关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fsDataInputStream);
    }

    /**
     * 分块读取HDFS上的大文件，比如根目录下的 /jdk-8u121-linux-x64.tar.gz
     * 下载第一块
     */
    @Test
    public void readFileSeek1() throws IOException {
        // 获取输入流
        FSDataInputStream fsDataInputStream = fs.open(new Path("/jdk-8u121-linux-x64.tar.gz"));
        // 创建输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\code_data\\hadoop\\jdk-8u121-linux-x64.tar.gz.part1"));

        // 流对流拷贝
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++){
            fsDataInputStream.read(buf);
            fileOutputStream.write(buf);
        }

        // 关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fsDataInputStream);

    }
    /**
     * 分块读取HDFS上的大文件，比如根目录下的 /jdk-8u121-linux-x64.tar.gz
     * 下载第二块
     * 下载完毕合并文件，在Window命令窗口中进入到目录D:\code_data\hadoop\，然后执行如下命令，对数据进行合并：
     * type jdk-8u121-linux-x64.tar.gz.part2 >> jdk-8u121-linux-x64.tar.gz.part1
     * 合并完成后，将jdk-8u121-linux-x64.tar.gz.part1重新命名为jdk-8u121-linux-x64.tar.gz。解压发现该tar包非常完整
     */
    @Test
    public void readFileSeek2() throws IOException {
        // 获取输入流
        FSDataInputStream fsDataInputStream = fs.open(new Path("/jdk-8u121-linux-x64.tar.gz"));
        // 定位数据输入位置
        fsDataInputStream.seek(1024*1024*128);
        // 创建输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\code_data\\hadoop\\jdk-8u121-linux-x64.tar.gz.part2"));

        // 流对流拷贝
        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, configuration);

        // 关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fsDataInputStream);
    }



}
