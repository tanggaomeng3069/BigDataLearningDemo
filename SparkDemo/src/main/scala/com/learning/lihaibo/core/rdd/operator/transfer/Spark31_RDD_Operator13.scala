package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark31_RDD_Operator13 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark31_RDD_Operator13")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,1,1,2,2,2),3)
        // TODO Spark - RDD 算子（方法）

        // TODO 缩减分区：coalesce
        rdd.coalesce(2)

        // TODO 扩大分区：repartition
        // repartition底层其实就是调用的coalesce，并且第二个参数为true
        rdd.repartition(6)



        sc.stop()

    }

}
