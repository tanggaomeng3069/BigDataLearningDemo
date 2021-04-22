package com.learning.nokerberos.mapreduce.wordcount2flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 10:13
 * @Description:
 * @Version 1.0
 */
public class JobSubmitter2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(JobSubmitter2.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("inputData/flow.log"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 设置参数：maptask在做数据分区时，用哪个分区逻辑，如果不指定，使用默认的HashPartitioner
        job.setPartitionerClass(ProvincePartitioner.class);
        // 由于Partitioner会产生六种结果，所以设置六种Reduce Task
        job.setNumReduceTasks(6);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);

    }
}
