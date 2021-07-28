package com.learning.nokerberos.mapreduce2.sgg12yasuo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/28 16:04
 * @Description:
 * @Version 1.0
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job
        Configuration conf = new Configuration();
        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        // SnappyCodec结合CentOS7.5以上版本系统使用，在Win10 IDEA无法运行
        conf.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);

        // 2 设置jar包路径
        job.setJarByClass(WordCountDriver.class);
        // 3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("inputData/inputword/*"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩格式
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
