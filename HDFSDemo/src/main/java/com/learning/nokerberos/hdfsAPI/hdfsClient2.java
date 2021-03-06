package com.learning.nokerberos.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/19 11:34
 * @Description:
 * @Version 1.0
 * <p>
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS  zookeeper
 */
public class hdfsClient2 {
    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群NN地址
        URI uri = new URI("hdfs://managerhd.bigdata:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");

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
     * 创建目录
     *
     * @throws IOException
     */
    @Test
    public void testmkdir() throws IOException {
        // 创建文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    /**
     * 上传
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml=> 在项目资源目录下的配置文件 => 代码里面的配置
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        fs.copyFromLocalFile(false, true, new Path("inputData/sunwukong.txt"), new Path("/xiyou/huaguoshan"));
    }

    @Test
    public void testPut2() throws IOException {
        FSDataOutputStream fos = fs.create(new Path("/input"));

        fos.write("hello world".getBytes());
    }

    /**
     * 文件下载
     *
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {
        // 参数的解读：参数一：原文件是否删除；参数二：原文件路径HDFS； 参数三：目标地址路径Win ; 参数四：是否不检测验证
        // fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("output"), false);
        fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("output"), true);

    }

    /**
     * 删除
     *
     * @throws IOException
     */
    @Test
    public void testRm() throws IOException {
        // 参数解读：参数1：要删除的路径； 参数2 ： 是否递归删除
        // 删除文件
//        fs.delete(new Path("/input"), false);

        // 删除空目录
//        fs.delete(new Path("/emptyDir"), false);

        // 删除非空目录
        fs.delete(new Path("/xiyou"), true);
    }

    /**
     * 文件的更名和移动
     */
    @Test
    public void testMv() throws IOException {
        // 参数解读：参数1 ：原文件路径； 参数2 ：目标文件路径
        // 对文件名称的修改
//        fs.rename(new Path("/input/word.txt"), new Path("/input/ss.txt"));

        // 对文件的移动和重命名
//        fs.rename(new Path("/input/ss.txt"), new Path("/cls.txt"));

        // 目录更名
        fs.rename(new Path("/input"), new Path("/output"));

    }

    /**
     * 获取文件详细信息
     *
     * @throws IOException
     */
    @Test
    public void fileDetail() throws IOException {

        // 获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        // 遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("==========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println(Arrays.toString(blockLocations));

        }
    }

    /**
     * 判断是文件夹还是文件
     *
     * @throws IOException
     */
    @Test
    public void testFile() throws IOException {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {

            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            } else {
                System.out.println("目录：" + status.getPath().getName());
            }
        }
    }


}
