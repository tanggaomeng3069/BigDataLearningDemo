package com.learning.nokerberos.mapreduce2.sgg8outputformat;

import com.learning.nokerberos.mapreduce2.sgg1wordcount.WordCountDriver;
import com.learning.nokerberos.mapreduce2.sgg1wordcount.WordCountMapper;
import com.learning.nokerberos.mapreduce2.sgg1wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/6/3 19:04
 * @Version 1.0
 * @Description:
 */
public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1.获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2.设置Jar包路径
        job.setJarByClass(LogDriver.class);

        // 3.关联mapper和reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5.设置最终的输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义outputformat
        job.setOutputFormatClass(LogOutputFormat.class);

        // 6.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("inputData/inputoutputformat/log.txt"));
        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 7.提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);


    }
}
