package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark29_RDD_Operator11 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark29_RDD_Operator11")
        val sc = new SparkContext(sparkConf)

//        val rdd: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2),2)
//        // TODO Spark - RDD 算子（方法）
//        //  [1,1,1], [2,2,2]
//        //  [], [2,2,2]
//        val filterRDD: RDD[Int] = rdd.filter(num => num%2==0)
//
//        // 多 => 少
//        // TODO 当数据过滤后，发现数据不够均匀，那么可以缩减分区
//        val coalesceRDD: RDD[Int] = filterRDD.coalesce(1)
//        coalesceRDD.saveAsTextFile("output")

        // TODO 如果发现数据分区不合理，也可以缩减分区
        val rdd: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2),6)
        val coalesceRDD: RDD[Int] = rdd.coalesce(2)
        coalesceRDD.saveAsTextFile("output")



        sc.stop()

    }

}
