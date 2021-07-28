package com.learning.nokerberos.mapreduce2.sgg12yasuo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/28 16:03
 * @Description:
 * @Version 1.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // atguigu, (1,1)
        // 累加
        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);

        // 写出
        context.write(key, outV);

    }
}
