package com.learning.nokerberos.mapreduce2.sgg9reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/6/18 17:06
 * @Description:
 * @Version 1.0
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        // 初始化 order pd
        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        // 1.获取一行数据
        String line = value.toString();

        // 2.判断数据是属于哪个文件的
        if (fileName.contains("order")){ // 处理的是订单表
            String[] split = line.split("\t");
            // 封装 k，v
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else { // 处理的是商品表
            String[] split = line.split("\t");
            // 封装 k，v
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }

        // 写出
        context.write(outK, outV);
    }
}
