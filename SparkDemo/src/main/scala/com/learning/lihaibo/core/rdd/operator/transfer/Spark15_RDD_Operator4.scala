package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark15_RDD_Operator4 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark15_RDD_Operator4")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）

        // 2个分区 => 143,256
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,6,5,4), 3)

        // 获取每个分区的最大值及分区号
        val rdd: RDD[(Int, Int)] = dataRDD.mapPartitionsWithIndex((index, iter) => {
            List((index, iter.max)).iterator
        })
        println(rdd.collect().mkString(","))


        sc.stop()

    }

}
