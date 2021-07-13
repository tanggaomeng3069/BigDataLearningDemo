package com.learning.lihaibo.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming08_Transform {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val ds: ReceiverInputDStream[String] = ssc.socketTextStream("10.180.210.232", 9999)

        // 方法1
//        val newDS: DStream[String] = ds.transform((rdd: RDD[String]) => {
//            rdd.map((_: String) * 2)
//        })
//        newDS.print()

        // 方法2
//        val newDS1: DStream[String] = ds.map((_: String)*2)
//        newDS1.print()
        // 方法1和方法2的输出结果一致

        // 方法1
        // Code Driver（此处的代码只会执行1次）
        val newDS: DStream[String] = ds.transform(
            rdd => {
                // Code Driver（此处的代码会执行N次，根据SparkStreaming的采集周期确定）
                // 一般此处的代码作用，监控
                // 周期性执行
                rdd.map(
                    data => {
                        // Code Executor（此处代码执行N次）
                        data * 2
                    }
                )
            }
        )
        newDS.print()

        // 方法2
        // Code Driver（此处代码执行1次）
        val newDS1: DStream[String] = ds.map(
            data => {
                // Code Executor（此处代码执行N次）
                data * 2
            }
        )
        newDS1.print()



        // TODO 关闭
        ssc.start()

        ssc.awaitTermination()

    }

}
