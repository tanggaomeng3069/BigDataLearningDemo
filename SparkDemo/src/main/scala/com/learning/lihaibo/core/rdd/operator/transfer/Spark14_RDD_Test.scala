package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark14_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark14_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  mapPartitions
        //  以分区为单位进行计算，和map算子很像
        //  区别就在于map算子是一个一个执行，而mapPartitions是一个分区一个分区执行
        //  类似批处理

        // map方法是全量数据操作，不能丢失数据
        // mapPartitions 一次性获取分区的所有数据，那么可以执行迭代器集合的所有操作
        //               过滤，max，sum

        // 2个分区 => 143,256
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,4,3,2,5,6), 2)

        // 获取每个分区的最大值
        val rdd: RDD[Int] = dataRDD.mapPartitions(iter => {
            List(iter.max).iterator
        })
        println(rdd.collect().mkString(","))

        sc.stop()

    }

}
