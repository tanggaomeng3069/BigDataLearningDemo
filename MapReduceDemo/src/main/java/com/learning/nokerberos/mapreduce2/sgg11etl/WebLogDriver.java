package com.learning.nokerberos.mapreduce2.sgg11etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/28 15:37
 * @Description:
 * @Version 1.0
 */
public class WebLogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取job 信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2.加载jar包
        job.setJarByClass(WebLogDriver.class);
        // 3.关联Map
        job.setMapperClass(WebLogMapper.class);
        // 4.设置最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reduce Task个数
        job.setNumReduceTasks(0);

        // 5.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("inputData/inputlog/web.log"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        // 6.提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
