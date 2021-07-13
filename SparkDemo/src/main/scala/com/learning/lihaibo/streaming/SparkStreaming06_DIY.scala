package com.learning.lihaibo.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming06_DIY {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 执行逻辑
        val ds: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("10.180.210.232", 9999))

        ds.print()

        // TODO 关闭
        ssc.start()

        ssc.awaitTermination()

    }

    // TODO 自定义数据采集器
    /**
      * 1. 集成Receiver，定义泛型，传递参数
      * 2. Receiver的构造方法有参数的，所以子类在继承时，应该传递这个参数
      * 3. 重写方法
      */
    class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

        private var socket: Socket = _

        def receive(): Unit = {
            val reader = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream,
                    "UTF-8"
                )
            )

            var s: String = null

            while (true) {

                s = reader.readLine()
                if (s != null) {
                    // TODO 将获取的数据保存到框架内部进行封装
                    store(s)
                }

            }


        }

        override def onStart(): Unit = {
            socket = new Socket(host, port)
            new Thread("Socket Receiver") {
                setDaemon(true)

                override def run() {
                    receive()
                }
            }.start()
        }

        override def onStop(): Unit = {
            socket.close()
            socket = null
        }
    }

}
