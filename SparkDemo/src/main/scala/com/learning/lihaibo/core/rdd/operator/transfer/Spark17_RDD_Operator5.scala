package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark17_RDD_Operator5 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark17_RDD_Operator5")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）

        val dataRDD: RDD[List[Int]] = sc.makeRDD(List(List(1,2), List(3,4)))

        val rdd: RDD[Int] = dataRDD.flatMap(list => list)

        println(rdd.collect().mkString(","))


        sc.stop()

    }

}
