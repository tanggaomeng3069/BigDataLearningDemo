package com.learning.lihaibo.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/13 9:21
  * Describe:
  */
object Spark_RDD_IO_Load {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

//        val dataRDD1: RDD[String] = sc.textFile("input1")
//        println(dataRDD1.collect().mkString(","))

//        val dataRDD2: RDD[Nothing] = sc.objectFile("input2")
//        println(dataRDD2.collect().mkString(","))

//        val dataRDD3: RDD[(Nothing, Nothing)] = sc.sequenceFile("input3")
//        println(dataRDD3.collect().mkString(","))

        sc.stop()

    }

}
