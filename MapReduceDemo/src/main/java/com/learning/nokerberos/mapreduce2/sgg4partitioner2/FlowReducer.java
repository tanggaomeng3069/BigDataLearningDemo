package com.learning.nokerberos.mapreduce2.sgg4partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/28 15:04
 * @Version 1.0
 * @Description:
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException,
            InterruptedException {
        //super.reduce(key, values, context);

        // 1.遍历集合累加值
        long totalUp = 0;
        long totalDown = 0;

        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }

        // 2.封装outk、outv
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        // 3.写出
        context.write(key, outV);

    }
}
