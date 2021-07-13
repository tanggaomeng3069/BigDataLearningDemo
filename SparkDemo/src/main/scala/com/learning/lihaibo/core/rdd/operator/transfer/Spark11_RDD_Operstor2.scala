package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark11_RDD_Operstor2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark11_RDD_Operstor2")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）

        // 2个分区 => 12,34
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // TODO 分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完毕后才会执行下一条数据
        //  分区间数据执行没有顺序，而且无需等待
        //  0 => (1,2) => 1A             1B 2A 2B
        //  1 => (3,4) =>    3A 3B 4A 4B


        val rdd1: RDD[Int] = rdd.map(x => {
            println("map A = " + x)
            x
        })

        val rdd2: RDD[Int] = rdd1.map(x => {
            println("map B = " + x)
            x
        })

        println(rdd2.collect().mkString(","))

        sc.stop()

    }

}
