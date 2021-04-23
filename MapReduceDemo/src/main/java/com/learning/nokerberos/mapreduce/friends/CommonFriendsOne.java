package com.learning.nokerberos.mapreduce.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 * @Date: 2021/4/23 10:36
 * @Description:
 * @Version 1.0
 */
public class CommonFriendsOne {
    public static class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] userFriends = value.toString().split(":");
            String user = userFriends[0];
            String[] friends = userFriends[1].split(",");

            v.set(user);
            for (String f : friends) {
                k.set(f);
                context.write(k, v);
            }
        }
    }

    public static class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text friend, Iterable<Text> users, Context context) throws IOException,
                InterruptedException {
            ArrayList<String> userList = new ArrayList<>();

            for (Text user : users) {
                userList.add(user.toString());
            }

            Collections.sort(userList);

            for (int i = 0; i < userList.size() - 1; i++) {
                for (int j = i + 1; j < userList.size(); j++) {
                    context.write(new Text(userList.get(i) + "-" + userList.get(j)), friend);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);

        job.setJarByClass(CommonFriendsOne.class);

        job.setMapperClass(CommonFriendsMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("inputData/friends.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

}
