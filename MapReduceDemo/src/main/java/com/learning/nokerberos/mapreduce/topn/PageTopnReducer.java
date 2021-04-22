package com.learning.nokerberos.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 11:27
 * @Description:
 * @Version 1.0
 */
public class PageTopnReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    TreeMap<PageCount, Object> treeMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        //super.reduce(key, values, context);
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        PageCount pageCount = new PageCount();
        pageCount.set(key.toString(), count);
        treeMap.put(pageCount, null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //super.cleanup(context);
        Configuration conf = context.getConfiguration();
        int topn = conf.getInt("top.n", 5);

        Set<Map.Entry<PageCount, Object>> entries = treeMap.entrySet();
        int i = 0;
        for (Map.Entry<PageCount, Object> entry : entries) {
            context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
            i++;
            if (i == topn) return;
        }

    }
}
