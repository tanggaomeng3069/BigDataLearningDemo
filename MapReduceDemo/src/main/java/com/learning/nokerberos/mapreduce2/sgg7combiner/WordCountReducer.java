package com.learning.nokerberos.mapreduce2.sgg7combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/23 15:09
 * @Description:
 * @Version 1.0
 * <p>
 * KEYIN, reducer阶段输入的key的类型：Text
 * VALUEIN, reducer阶段输入的value的类型：IntWritable
 * KEYOUT, reducer阶段输出的key的类型： Text
 * VALUEOUT, reducer阶段输出的value的类型： IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        //super.reduce(key, values, context);
        int sum = 0;
        // 累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);

        context.write(key, outV);
    }
}
