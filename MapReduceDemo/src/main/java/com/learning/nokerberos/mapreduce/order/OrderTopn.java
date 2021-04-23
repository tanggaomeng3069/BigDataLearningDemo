package com.learning.nokerberos.mapreduce.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/23 10:01
 * @Description:
 * @Version 1.0
 */
public class OrderTopn {

    public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderTopn> {
        OrderBean orderBean = new OrderBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
            k.set(fields[0]);

            // 从这里交给maptask的kv对象，会被maptask序列化后存储，所以不用担心覆盖的问题
            context.write(k, orderBean);
        }
    }

    public static class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException,
                InterruptedException {
            //super.reduce(key, values, context);
            // 获取topn的参数
            int topn = context.getConfiguration().getInt("order.top.n", 3);

            ArrayList<OrderBean> arrayList = new ArrayList<>();
            // reducer task 提供的values迭代器，每次迭代返回给我们的都是同一个对象，只是set了不同的值
            for (OrderBean value : values) {
                // 构造一个新的对象，来存储本次迭代出来的值
                OrderBean newBean = new OrderBean();
                newBean.set(value.getOrderId(), value.getUserId(), value.getPdtName(), value.getPrice(),
                        value.getNumber());
                arrayList.add(newBean);
            }
            // 对arrayList中的orderBean对象排序，按总金额大小倒序排列，如果总金额相同，则对比商品名称
            Collections.sort(arrayList);

            for (int i = 0; i < topn; i++) {
                context.write(arrayList.get(i), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(OrderTopn.class);

        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("inputData/orderData.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        job.setNumReduceTasks(2);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }

}
