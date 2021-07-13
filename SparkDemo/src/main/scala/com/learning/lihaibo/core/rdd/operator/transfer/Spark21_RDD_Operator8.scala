package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark21_RDD_Operator8 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark21_RDD_Operator8")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 3)

        // TODO filter 过滤
        val rdd: RDD[Int] = dataRDD.filter(num => {
            num % 2 == 0
        })
        println(rdd.collect().mkString(","))



        sc.stop()

    }

}
