package com.learning.xiaoniuxuetang.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/9/17 15:03
  * Describe:
  */
object streamingWordCount {

  def main(args: Array[String]): Unit = {

    // 离线任务创建SparkContext，实时计算使用StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("streamingWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // StreamingContext是对SparkContext的包装，包装了一层就增加了实时的功能
    // 第二个参数是小批次产生的时间间隔
    val ssc = new StreamingContext(sc, Milliseconds(5000))

    // 有了StreamingContext，就可以创建SparkStreaming的抽象DStream
    // 从一个socket端口中读取数据
    // 在Linux中安装nc，CentOS7 yum install nmap
    // 在Linux端执行 nc -lk 8888 启动一个Server，启动一个client 监听8888端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("10.180.210.232", 8888)

    // 对DStream进行操作，操作这个抽象（代理，描述），就像操作一个本地集合一样
    // 切分压平
    val words: DStream[String] = lines.flatMap((_: String).split(" "))
    // 单词和1组合
    val wordAndOne: DStream[(String, Int)] = words.map(((_: String),1))
    // 聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey((_: Int)+(_: Int))
    // 打印结果（Action）
    reduced.print()

    // 启动SparkStreaming程序
    ssc.start()

    // 等待优雅的退出
    ssc.awaitTermination()


  }

}
