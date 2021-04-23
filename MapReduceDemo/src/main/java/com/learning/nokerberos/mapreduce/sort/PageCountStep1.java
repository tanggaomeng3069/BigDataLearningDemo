package com.learning.nokerberos.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 15:01
 * @Description:
 * @Version 1.0
 */
public class PageCountStep1 {

    public static class PageCountStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            String line = value.toString();
            String[] fields = line.split(" ");
            context.write(new Text(fields[1]), new IntWritable(1));
        }
    }

    public static class PageCountStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            //super.reduce(key, values, context);
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageCountStep1.class);

        job.setMapperClass(PageCountStep1Mapper.class);
        job.setReducerClass(PageCountStep1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("inputData/request.dat"));
        FileOutputFormat.setOutputPath(job, new Path("output1"));

        job.setNumReduceTasks(3);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
