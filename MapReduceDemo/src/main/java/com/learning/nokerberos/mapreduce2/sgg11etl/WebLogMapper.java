package com.learning.nokerberos.mapreduce2.sgg11etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/28 15:25
 * @Description:
 * @Version 1.0
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.获取一行数据
        String line = value.toString();
        // 2.解析日志行
        boolean result = parseLog(line, context);
        // 3.日志不合法退出
        if (!result){
            return;
        }
        // 4.日志合法，直接输出
        context.write(value, NullWritable.get());

    }

    // 2 封装解析日志的方法
    private boolean parseLog(String line, Context context){
        // 1.截取
        String[] fields = line.split(" ");

        // 2.日志的长度大于11的合法
//        if (fields.length > 11) {
//            return true;
//        } else {
//            return false;
//        }
        // 2.可以简写成为
        return fields.length > 11;

    }

}
