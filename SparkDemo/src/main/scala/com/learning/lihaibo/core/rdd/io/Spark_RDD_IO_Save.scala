package com.learning.lihaibo.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/13 9:21
  * Describe:
  */
object Spark_RDD_IO_Save {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1),("b", 2),("c", 3)))

//        dataRDD.saveAsTextFile("output1")
//        dataRDD.saveAsObjectFile("output2")
//        dataRDD.saveAsSequenceFile("output3")


        sc.stop()

    }

}
