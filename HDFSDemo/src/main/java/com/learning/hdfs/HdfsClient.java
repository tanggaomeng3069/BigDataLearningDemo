package com.learning.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/16 14:47
 * @Description:
 * @Version 1.0
 */
public class HdfsClient {

    /**
     * 在HDFS上指定用户，创建指定目录
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testMkdir() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        // 客户端去操作hdfs时，是有一个用户身份，
        // 默认情况下，hdfs客户端api会采用window当前用户访问hdfs，会报权限异常错误
        // 所以在使用hdfs客户端访问hdfs集群时，一定要配置访问用户信息
        FileSystem fs = FileSystem.get(new URI("hdfs://managerhd.bigdata:8020"), configuration, "zhengzhou");

        //2.创建目录
        fs.mkdirs(new Path("/zhengzhou"));

        //3.关闭资源
        fs.close();

    }

    @Test
    public void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        // 设置双副本
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://managerhd.bigdata:8020"), configuration, "zhengzhou");

        //2.上传文件
        fs.copyFromLocalFile(new Path("inputData/sunwukong.txt"), new Path("/zhengzhou"));

        //3.关闭资源
        fs.close();

    }


}
