package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming13_Continue {

    def main(args: Array[String]): Unit = {

        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", getStreamingContext)


        // TODO 关闭
        ssc.start()
        ssc.awaitTermination()

    }

    def getStreamingContext(): StreamingContext = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 采集周期：3s
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        // TODO 处理逻辑


        ssc
    }

}
