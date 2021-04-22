package com.learning.nokerberos.mapreduce.wordcount2flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 10:37
 * @Description:
 * @Version 1.0
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        String[] fields = line.split("\t");

        String phone = fields[1];
        int upFlow = Integer.parseInt(fields[fields.length - 3]);
        int dFlow = Integer.parseInt(fields[fields.length - 2]);

        context.write(new Text(phone), new FlowBean(phone, upFlow, dFlow));
    }
}
