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
object SparkStreaming05_File {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 执行逻辑
        //  监控目录in，如果目录中新建文件，就会自动读取内容
        //  注意：但是如果使用旧得文件移动到in目录，则会导致识别不出得现象
        //  稳定性不好
        val dirDS: DStream[String] = ssc.textFileStream("in")

        dirDS.print()

        // TODO 关闭
        ssc.start()

        ssc.awaitTermination()

    }

}
