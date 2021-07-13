package com.learning.lihaibo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Author: tanggaomeng
  * Date: 2021/1/25 20:00
  * Describe:
  */
object SparkStreaming09_State {

    def main(args: Array[String]): Unit = {

        // TODO SparkStreaming环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 以下两个设置检查点的方法相同
        ssc.checkpoint("cp")
//        ssc.sparkContext.setCheckpointDir("cp")

        val ds: ReceiverInputDStream[String] = ssc.socketTextStream("10.180.210.232", 9999)

        // TODO 数的有状态保存
        //  将Spark每个采集周期数据的处理结果保存起来，然后和后续的数据进行聚合
        //  reduceByKey是无状态的，而我们需要的是有状态的数据操作
        //  有状态的目的其实就是将每一个采集周期数据的计算结果临时保存起来
        //  然后在下一次数据的处理中可以继续使用

        ds
            .flatMap((_: String).split(" "))
            .map((_: String, 1L))
            //.reduceByKey((_: Int) + (_: Int))
            // updateStateByKey是有状态的计算方法
            // 第一个参数表示 相同的key的value的集合
            // 第二个参数表示 相同的key的缓冲区的数据，有可能为空
            // 这里的计算的中间结果需要保存到检查点的位置中，所以需要设定检查点路径
            .updateStateByKey[Long](
                (seq:Seq[Long], buffer:Option[Long]) => {
                    val newBufferValue: Long = buffer.getOrElse(0L) + seq.sum
                    Option(newBufferValue)
                }
            )
            .print()


        // TODO 关闭
        ssc.start()

        ssc.awaitTermination()

    }

}
