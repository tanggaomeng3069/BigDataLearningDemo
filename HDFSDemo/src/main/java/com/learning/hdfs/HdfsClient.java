package com.learning.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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

    /**
     * HDFS文件上传，执行副本数
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        // 设置双副本
        // 参数优先级：
        // 1.客户端代码中设置的值 > 2.ClassPath下的用户自定义配置文件（resources）
        // > 3.服务器自定义配置（xxx-site.xml） > 4.服务器的默认配置（xxx-default.xml）
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://managerhd.bigdata:8020"), configuration, "zhengzhou");

        //2.上传文件
        // 参数一：表示是否删除原数据
        // 参数二：表示是否允许覆盖目标文件
        // 参数三：原数据路径
        // 参数四：目的路径
        fs.copyFromLocalFile(false, true, new Path("inputData/sunwukong.txt"), new Path("/zhengzhou"));

        //3.关闭资源
        fs.close();

    }

    /**
     * HDFS下载文件到本地
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://managerhd.bigdata:8020"), configuration, "zhengzhou");

        //2.执行下载操作
        // 参数一：boolean delSrc 指是否将原文件删除
        // 参数二：Path src 指要下载的文件路径
        // 参数三：Path dst 指将文件下载到的路径
        // 参数四：boolean useRawLocalFileSystem 是否不开启文件校验（true：不开启）
        // 注意：如果本地没有目录，直接下载并修改名称，如果本地存在目录，则下载到该目录下，不修改其文件名称
        fs.copyToLocalFile(false, new Path("/zhengzhou/sunwukong.txt"), new Path("./output"), true);

        //3.关闭资源
        fs.close();

    }

    /**
     * 删除HDFS文件和目录
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testDelete() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://managerhd.bigdata:8020"), configuration, "zhengzhou");

        //2.执行删除
        // 参数一：要删除的路径
        // 参数二：是否递归删除
        fs.delete(new Path("/zhengzhou"), true);

        // 场景
        // 删除文件，参数二：false
        // 删除空目录：参数二：false
        // 删除非空目录：参数二：true

        //3.关闭资源
        fs.close();

    }

    /**
     * HDFS文件详细信息查看
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testListFiles() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://managerhd.bigdata:8020"), configuration, "zhengzhou");

        //2.获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "========");
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

        //3.关闭资源
        fs.close();

    }

    /**
     * HDFS文件和文件夹的判断
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testListStatus() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://managerhd.bigdata:8020"), configuration, "zhengzhou");

        //2.判断是文件还是文件夹
        FileStatus[] listStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus: listStatuses) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            } else { // 否则是目录
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        //3.关闭资源
        fs.close();

    }


}
