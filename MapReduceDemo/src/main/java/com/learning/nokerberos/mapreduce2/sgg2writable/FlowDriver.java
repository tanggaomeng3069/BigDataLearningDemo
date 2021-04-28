package com.learning.nokerberos.mapreduce2.sgg2writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/28 15:08
 * @Version 1.0
 * @Description:
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1.获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2.设置Jar
        job.setJarByClass(FlowDriver.class);

        // 3.关联Mapper、Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4.设置Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5.设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6.设置数据的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("inputData/inputflow/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 7.提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
