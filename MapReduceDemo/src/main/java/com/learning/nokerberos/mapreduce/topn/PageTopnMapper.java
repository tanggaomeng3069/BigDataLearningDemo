package com.learning.nokerberos.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 11:27
 * @Description:
 * @Version 1.0
 */
public class PageTopnMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        String[] fields = line.split(" ");
        context.write(new Text(fields[1]), new IntWritable(1));
    }
}
