package com.learning.nokerberos.mapreduce2.sgg1wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/23 15:09
 * @Description:
 * @Version 1.0
 * <p>
 * KEYIN, map阶段输入的key的类型：LongWritable
 * VALUEIN, map阶段输入的value的类型：Text
 * KEYOUT, map阶段输出的key的类型： Text
 * VALUEOUT, map阶段输出的value的类型： IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text outK = new Text();
    private final IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        // 1.获取一行
        String line = value.toString();

        // 2.切割
        String[] words = line.split(" ");

        // 3.循环写出
        for (String word : words) {
            // 封装 outK
            outK.set(word);
            // 写出
            context.write(outK, outV);
        }

    }
}
