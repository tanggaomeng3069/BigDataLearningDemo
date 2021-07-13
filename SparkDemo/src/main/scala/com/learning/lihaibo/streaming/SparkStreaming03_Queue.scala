package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming03_Queue {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 执行逻辑
        val queue = new mutable.Queue[RDD[String]]()
        val queueDS: InputDStream[String] = ssc.queueStream(queue)

        queueDS.print()

        // TODO 关闭
        ssc.start()

        for (i <- 1 to 5) {
            val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString))
            queue.enqueue(rdd)
            Thread.sleep(1000)
        }

        ssc.awaitTermination()

    }

}
