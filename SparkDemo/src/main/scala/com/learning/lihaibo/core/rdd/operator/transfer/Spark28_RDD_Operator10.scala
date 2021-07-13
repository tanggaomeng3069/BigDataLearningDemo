package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark28_RDD_Operator10 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark27_RDD_Operator9")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,1,2,4))
        // TODO Spark - RDD 算子（方法）
        //  distinct: 去重
        val dataRDD: RDD[Int] = rdd.distinct()
        println(dataRDD.collect().mkString(","))

        // distinct可以改变分区的数量
        val dataRDD1: RDD[Int] = rdd.distinct(2)
        println(dataRDD1.collect().mkString(","))

        sc.stop()

    }

}
