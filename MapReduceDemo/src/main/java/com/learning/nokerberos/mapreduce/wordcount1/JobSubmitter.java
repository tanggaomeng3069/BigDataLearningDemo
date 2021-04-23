package com.learning.nokerberos.mapreduce.wordcount1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 8:46
 * @Description:
 * @Version 1.0
 */
public class JobSubmitter {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();
        // 设置job运行时要访问的默认文件系统
        //configuration.set("fs.defaultFS", "hdfs://managerhd.bigdata:8020");
        // 设置job提交到yarn运行
        //configuration.set("mapreduce.framework.name","yarn");
        //configuration.set("yarn.resourcemanager.hostname","managerhd.bigdata");
        // 如果要从Window系统上运行这job提交客户端程序，则需要加这个跨平台提交的参数
        //configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("mapreduce.map.cpu.vcores", "2");

        Job job = Job.getInstance(configuration);
        // 设置job名称
        job.setJobName("WordCount");
        // 封装参数：jar包所在的位置
        //job.setJar("f:/wc.jar");
        job.setJarByClass(JobSubmitter.class);

        // 封装参数：本次job所要调用Mapper实现类，Reducer实现类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 封装参数：本次job要输入数据集所在路径、输出结果的输出路径
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("inputData/test1.txt"));
        // 运行程序时，此output目录不能存在，程序会自动生成
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 压缩
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // 封装参数：自定义reduce task的数量
        job.setNumReduceTasks(2);

        // 提交job给yarn
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
