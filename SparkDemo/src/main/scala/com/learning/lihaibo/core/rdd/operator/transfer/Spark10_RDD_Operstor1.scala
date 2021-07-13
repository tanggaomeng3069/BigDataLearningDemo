package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark10_RDD_Operstor1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark10_RDD_Operstor1")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）

        // 2个分区 => 12,34
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // TODO 分区问题
        //  RDD中有分区列表
        //  RDD经过算子转换后，默认分区是不变的，数据会转换后输出

        val rdd1: RDD[Int] = rdd.map(_ * 2 )

        rdd1.saveAsTextFile("output")

        sc.stop()

    }

}
