package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark25_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark25_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  WordCount

        val dataRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello"))

        println(dataRDD.flatMap(_.split(" "))
          .groupBy(word => word)
          .map(kv => (kv._1, kv._2.size))
          .collect()
          .mkString(","))

        sc.stop()

    }

}
