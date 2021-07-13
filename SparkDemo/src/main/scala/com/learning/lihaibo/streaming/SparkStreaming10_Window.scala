package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming10_Window {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 采集周期：3s
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // nc -lp 9999
        val ds: ReceiverInputDStream[String] = ssc.socketTextStream("10.180.210.232", 9999)

        // TODO 窗口
        val wordDS: DStream[String] = ds.flatMap((_: String).split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map((_: String, 1))

        // TODO 将多个采集周期作为计算的整体
        //  窗口的范围应该是采集周期的整数倍
        //  默认滑动的幅度（步长）为一个采集周期
        //val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(9))

        //  窗口的计算周期，等同于窗口的滑动步长
        //  窗口的范围大小和滑动的步长，应该都是采集周期的整数倍
        val windowDS: DStream[(String, Int)] = wordToOneDS.window(Seconds(9), Seconds(6))

        val result: DStream[(String, Int)] = windowDS.reduceByKey((_: Int) + (_: Int))

        result.print()

        // TODO 关闭
        ssc.start()

        ssc.awaitTermination()

    }

}
