package com.learning.nokerberos.mapreduce2.sgg4partitioner2;

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
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private final Text outK = new Text();
    private final FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        // 1.获取一行：
        // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String line = value.toString();

        // 2.切割
        String[] split = line.split("\t");

        // 3.抓取想要的数据
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        // 4.封装
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        // 5.写出
        context.write(outK, outV);

    }
}
