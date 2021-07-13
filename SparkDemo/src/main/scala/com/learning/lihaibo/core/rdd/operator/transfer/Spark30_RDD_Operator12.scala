package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark30_RDD_Operator12 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark30_RDD_Operator12")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2),2)
        // TODO Spark - RDD 算子（方法）

        // 扩大分区
        // coalesce主要目的是缩减分区，扩大分区时没有效果
        // 为什么不能扩大分区？？
        // 因为在缩减分区时，数据不会打乱重写组合，没有shuffle的过程。
//        val coalesceRDD: RDD[Int] = rdd.coalesce(6)
//        coalesceRDD.saveAsTextFile("output")

        // 如果就是非要将数据扩大分区，那么必须打乱数据后重新组合，必须使用shuffle
        // TODO coalesce方法的第一个参数表示缩减分区后的分区数量
        //      coalesce方法的第二个参数表示分区改变时，是否会打乱重新组合数据，默认false不打乱
        val coalesceRDD1: RDD[Int] = rdd.coalesce(6, true)
        coalesceRDD1.saveAsTextFile("output1")




        sc.stop()

    }

}
