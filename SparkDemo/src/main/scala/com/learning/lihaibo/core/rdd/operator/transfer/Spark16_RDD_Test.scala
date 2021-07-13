package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark16_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark16_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        // 获取第二个分区的数据
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,4,3,2,5,6), 3)

        // 获取的分区索引从0开始
        val rdd: RDD[Int] = dataRDD.mapPartitionsWithIndex((index, iter) => {
            if (index == 1) {
                iter
            } else {
                Nil.iterator
            }
        })
        println(rdd.collect().mkString(","))

        sc.stop()

    }

}
