package com.learning.lihaibo.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 10:14
  * Describe:
  */
object Spark01_RDD_Memory {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Memory")
        val sc = new SparkContext(sparkConf)

        // TODO Spark从内存中创建RDD
        val list = List(1, 2, 3, 4)

        val rdd: RDD[Int] = sc.parallelize(list)
        println(rdd.collect().mkString(","))

        // TODO makeRDD的底层代码其实就是调用了parallelize方法
        val rdd1: RDD[Int] = sc.makeRDD(list)
        println(rdd1.collect().mkString("-"))


        sc.stop()

    }

}
