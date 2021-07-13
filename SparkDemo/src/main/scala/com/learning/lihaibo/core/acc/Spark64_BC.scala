package com.learning.lihaibo.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Author: tanggaomeng
  * Date: 2021/1/12 15:45
  * Describe:
  */
object Spark64_BC {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO Spark广播变量
        val dataRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val dataRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

        val joinRDD: RDD[(String, (Int, Int))] = dataRDD1.join(dataRDD2)

        println(joinRDD.collect().mkString(","))

        sc.stop()

    }
}
