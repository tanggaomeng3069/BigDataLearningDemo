package com.learning.nokerberos.mapreduce2.sgg3combineTextInputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/23 15:09
 * @Description:
 * @Version 1.0
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1.获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2.设置Jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3.关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终的输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
        // 使用TextInputFormat，MapTask数量：4       ->   number of splits:4

        // 使用CombineTextInputFormat，MapTask数量：3    ->   number of splits:3
//        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置 4M   ->   number of splits:3
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // CombineTextInputFormat 主要解决小文件的处理，如果把虚拟切片设置为 20M，则MapTask的数量会变成 1
//        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置 20M   ->   number of splits:1
//        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 6.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("inputData/inputcombinetextinputformat/*"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 7.提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
