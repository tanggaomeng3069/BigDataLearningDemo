package com.learning.nokerberos.mapreduce2.sgg5writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/28 15:04
 * @Version 1.0
 * @Description:
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        //super.reduce(key, values, context);

        for (Text value : values) {
            context.write(value, key);
        }

    }
}
