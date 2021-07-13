package com.learning.lihaibo.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author: tanggaomeng
  * Date: 2021/1/12 15:45
  * Describe:
  */
object Spark66_BC2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO Spark广播变量
        //  广播变量：分布式共享只读变量

        val dataRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val list = List(("a", 4), ("b", 5), ("c", 6))

        val bcList: Broadcast[List[(String, Int)]] = sc.broadcast(list)

        val RDD2: RDD[(String, (Int, Int))] = dataRDD1.map {
            case (word, count1) => {
                var count2 = 0
                for (kv <- bcList.value) {
                    val w: String = kv._1
                    val v: Int = kv._2
                    if (w == word) {
                        count2 = v
                    }
                }
                (word, (count1, count2))
            }
        }
        println(RDD2.collect().mkString(","))

        sc.stop()

    }
}
