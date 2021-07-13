package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark13_RDD_Operstor3 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark13_RDD_Operstor3")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  mapPartitions
        //  以分区为单位进行计算，和map算子很像
        //  区别就在于map算子是一个一个执行，而mapPartitions是一个分区一个分区执行
        //  类似批处理

        // map方法是全量数据操作，不能丢失数据
        // mapPartitions 一次性获取分区的所有数据，那么可以执行迭代器集合的所有操作
        //               过滤，max，sum

        // 2个分区 => 12,34
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

//        val rdd: RDD[Int] = dataRDD.mapPartitions(iter => {
//            iter.foreach(println)
//            iter
//        })
//        rdd.collect()

//        val rdd: RDD[Int] = dataRDD.mapPartitions(iter => {
//            iter.map(_ * 2)
//        })
//        println(rdd.collect().mkString(","))

        val rdd: RDD[Int] = dataRDD.mapPartitions(iter => {
            iter.filter(_ % 2 == 0)
        })
        println(rdd.collect().mkString(","))


        sc.stop()

    }

}
