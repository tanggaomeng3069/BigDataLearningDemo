package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming04_Queue1 {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 执行逻辑
        val queue = new mutable.Queue[RDD[Int]]()

        val queueDS: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)
        val mappedStream: DStream[(Int, Int)] = queueDS.map(((_: Int), 1))
        val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey((_: Int) + (_: Int))

        reducedStream.print()

        // TODO 关闭
        ssc.start()

        for (i <- 1 to 5) {
            queue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()

    }

}
