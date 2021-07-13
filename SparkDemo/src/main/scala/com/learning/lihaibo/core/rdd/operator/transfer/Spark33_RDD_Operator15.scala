package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark33_RDD_Operator15 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark33_RDD_Operator15")
        val sc = new SparkContext(sparkConf)

        val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
        val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6), 2)

        // TODO 并集，数据合并，分区也会合并
        val rdd3: RDD[Int] = rdd1.union(rdd2)
        //println(rdd3.collect().mkString(","))
        rdd3.saveAsTextFile("output3")

        // TODO 交集，保留最大的分区数，数据会被打乱重组，shuffle
        val rdd4: RDD[Int] = rdd1.intersection(rdd2)
        //println(rdd4.collect().mkString(","))
        rdd4.saveAsTextFile("output4")

        // TODO 差集，数据会被打乱重组，shuffle
        //  当调用rdd的subtract方法时，以当前rdd的分区为主，所以分区数量等于当前rdd的分区数量
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        //println(rdd5.collect().mkString(","))
        rdd5.saveAsTextFile("output5")

        // TODO 拉链，分区数不变；要求两个RDD的分区数相同，并且每个分区的数据量相同
        //  2个RDD分区不一致的场合：报错抛出异常；
        //  2个RDD分区不一致，数据量也不相同，但是每个分区的数据量一致：报错抛出异常；
        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        //println(rdd6.collect().mkString(","))
        rdd6.saveAsTextFile("output6")

        sc.stop()

    }

}
