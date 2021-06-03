package com.learning.nokerberos.mapreduce2.sgg6partitionerandwritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/28 14:53
 * @Version 1.0
 * @Description:
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private final FlowBean outK = new FlowBean();
    private final Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        String line = value.toString();
        String[] split = line.split("\t");

        // 封装
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();
        outV.set(split[0]);

        context.write(outK, outV);


    }
}
