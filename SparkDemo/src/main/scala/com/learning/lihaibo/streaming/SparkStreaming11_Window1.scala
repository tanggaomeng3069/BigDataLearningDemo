package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming11_Window1 {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 采集周期：3s
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        // nc -lp 9999
        val ds: ReceiverInputDStream[String] = ssc.socketTextStream("10.180.210.232", 9999)

        // TODO 窗口
        val wordToMap: DStream[(String, Int)] = ds.map((num: String) => ("key", num.toInt))

        // reduceByKeyAndWindow方法一般用在重复数据的范围比较大的场合，这样可以优化效率
        val result: DStream[(String, Int)] = wordToMap.reduceByKeyAndWindow(
            (x: Int, y: Int) => {
                println(s"x = ${x}, y = ${y}")
                x + y
            },
            (a: Int, b: Int) => {
                println(s"a = ${a}, b = ${b}")
                a - b
            },
            Seconds(9)
        )

        result.foreachRDD((rdd: RDD[(String, Int)]) => rdd.foreach(println))


        // TODO 关闭
        ssc.start()

        ssc.awaitTermination()

    }

}
