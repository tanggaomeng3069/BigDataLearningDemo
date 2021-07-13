package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark22_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark22_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)

        // 将每个分区转换为数组
        val glomRDD: RDD[Array[Int]] = dataRDD.glom()

        // 将数组重的最大值求出
        val maxRDD: RDD[Int] = glomRDD.map(array => array.max)
        
        val array: Array[Int] = maxRDD.collect()

        println(array.sum)

        sc.stop()

    }

}
