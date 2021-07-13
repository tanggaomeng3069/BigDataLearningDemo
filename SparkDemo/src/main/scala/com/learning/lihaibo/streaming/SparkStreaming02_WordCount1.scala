package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming02_WordCount1 {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        //  StreamingContext创建时，需要传递两个参数
        //  第一个参数表示环境配置
        //  第二个参数表示批处理的周期（采集周期）
        //  SparkStreaming使用核数最少是2个
        val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 执行逻辑
        //  获取端口数据，从socket获取数据，一行一行获取的
        //  在Linux中安装nc（netcat），CentOS7 yum install nmap
        //  在Linux端执行 nc -lk 9999 启动一个Server，启动一个client 监听8888端口
        //  如果在Window下使用（netcat-win32-1.12.zip），命令：nc -lp 9999
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("10.180.210.232", 9999)

        val wordDS: DStream[String] = socketDS.flatMap((_: String).split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map((word: String) => (word, 1))
        val wordToSum: DStream[(String, Int)] = wordToOneDS.reduceByKey((_: Int) + (_: Int))

        wordToSum.print()

        // TODO 关闭
        //  由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
        //  如果main方法执行完毕，应用程序也会自动结束，所以不能让main在执行完毕。
        //  不能使用 ssc.stop() 关闭SparkStreaming

        // TODO 正确的关闭方式
        //  1.启动采集器
        ssc.start()

        //  2.等待采集器的关闭
        ssc.awaitTermination()

    }

}
