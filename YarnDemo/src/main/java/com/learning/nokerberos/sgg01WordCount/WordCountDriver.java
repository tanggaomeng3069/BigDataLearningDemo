package com.learning.nokerberos.sgg01WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;


/**
 * @Author: tanggaomeng
 * @Date: 2021/7/29 16:13
 * @Description: 运行命令：
 * yarn jar YarnDemo.jar com.atguigu.yarn.WordCountDriver wordcount /input /output
 * @Version 1.0
 */
public class WordCountDriver {
    private static Tool tool;

    public static void main(String[] args) throws Exception {
        // 创建配置
        Configuration conf = new Configuration();

        if ("wordcount".equals(args[0])) {
            tool = new WordCount();
        } else {
            throw new RuntimeException("no such tool " + args[0]);
        }
        // if-else 可以修改为switch
//        switch (args[0]) {
//            case "wordcount":
//                tool = new WordCount();
//                break;
//            default:
//                throw new RuntimeException("no such tool " + args[0]);
//        }

        // 执行程序
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}
