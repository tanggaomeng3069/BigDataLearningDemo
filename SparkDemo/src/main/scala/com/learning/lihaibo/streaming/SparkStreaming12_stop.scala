package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming12_stop {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 采集周期：3s
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // nc -lp 9999
        val ds: ReceiverInputDStream[String] = ssc.socketTextStream("10.180.210.232", 9999)

        // TODO 窗口
        val wordToMap: DStream[(String, Int)] = ds.map((num: String) => ("key", num.toInt))

        wordToMap.print()

        //Stop(X)

        // TODO 关闭
        ssc.start()

        //Stop(X)

        // TODO 当业务升级的场合，或逻辑发生变化
        //  Stop方法一般不会放置在main方法线程完成
        //  需要将stop方法使用新的线程完成调用

        new Thread(new Runnable {
            override def run(): Unit ={
                // TODO Stop方法的调用不应该是线程启动后立马调用
                //  Stop方法的调用时机，这个时机不容易确定，需要周期性的判断时机是否出现
                while (true){
                    Thread.sleep(5000)
                    // TODO 关闭时机的判断一般不会使用业务操作
                    //  一般采用第三方的程序或存储进行判断
                    //  HDFS => /stopSpark
                    //  zk
                    //  mysql
                    //  redis
                    //  优雅的关闭
                    val state: StreamingContextState = ssc.getState()
                    if (state == StreamingContextState.ACTIVE){
                        ssc.stop(stopSparkContext = true, stopGracefully = true)

                        // TODO SparkStreaming如果停止执行后，当前的线程也应该同时停止
                        System.exit(0)
                    }

                }

            }
        }).start()

        ssc.awaitTermination()

        //Stop(X)


        // 线程
//        val t = new Thread()
//        t.start()
//        t.stop() // 数据安全，线程安全，已弃用


    }

}
