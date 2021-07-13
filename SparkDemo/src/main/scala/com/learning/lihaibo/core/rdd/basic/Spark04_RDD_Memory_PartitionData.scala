package com.learning.lihaibo.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 14:38
  * Describe:
  */
object Spark04_RDD_Memory_PartitionData {

    def main(args: Array[String]): Unit = {

        // TODO Scala语法



        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark04_RDD_Memory_PartitionData")
        val sc = new SparkContext(sparkConf)

        // TODO 内存中的集合数据按照平均分配的方式进行分区处理
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
        // 12,34
        rdd.saveAsTextFile("output")

        val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4), 4)
        // 1,2,3,4
        rdd1.saveAsTextFile("output1")

        // TODO saveAsTextFile方法如果文件路径已经存在，会发生错误
        // TODO 内存中的集合数据如果不能平均分配，会将多余的数据放置在最后一个分区
        val rdd2: RDD[Int] = sc.makeRDD(List(1,2,3,4), 3)
        // 1,2,34
        rdd2.saveAsTextFile("output2")

        // TODO 内存中的数据的分配基本上就是平均分配，如果不能整除，会采用一个基本的算法实现分配

        // List(1,2,3,4,5) => Array(1,2,3,4,5)
        // (length = 5, num = 3)
        // (0, 1, 2)
        // => 0 => (0, 1) => 1
        // => 1 => (1, 3) => 2,3
        // => 2 => (3, 5) => 4,5
        // Array.slice => 切分数组 => (from, until)

        val rdd3: RDD[Int] = sc.makeRDD(List(1,2,3,4,5), 3)
        // 1,23,45
        rdd3.saveAsTextFile("output3")

        sc.stop()

    }

}
