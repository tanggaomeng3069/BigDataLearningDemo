package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark42_RDD_Operator24 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark42_RDD_Operator24")
        val sc = new SparkContext(sparkConf)

        // TODO sortByKey

        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("c",3),("b",2)))

//        val sortRDD: RDD[(String, Int)] = dataRDD.sortByKey()
        val sortRDD: RDD[(String, Int)] = dataRDD.sortByKey(true)

        println(sortRDD.collect().mkString(","))

        sc.stop()

    }

}
