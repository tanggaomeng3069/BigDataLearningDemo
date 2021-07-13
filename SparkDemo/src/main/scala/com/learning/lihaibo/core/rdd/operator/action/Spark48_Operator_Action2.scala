package com.learning.lihaibo.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 13:37
  * Describe:
  */
object Spark48_Operator_Action2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 算子 - 行动
        val dataRDD: RDD[Int] = sc.makeRDD(List(2,1,4,3))

        // TODO takeOrdered
        // 2,1,4,3 => 1,2,3 => 1,2,3 先排序，再取前三个(true)
        // 2,1,4,3 => 2,1,4 => 1,2,4 先取前三个，再排序(false)
        val ints: Array[Int] = dataRDD.takeOrdered(3)
        println(ints.mkString(","))
        // 1,2,3

        sc.stop()

    }

}
