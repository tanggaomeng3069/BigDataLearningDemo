package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark09_RDD_Operstor {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark09_RDD_Operstor")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  转换算子
        //  能够将旧的RDD通过方法转换为新的RDD，但是不会触发作业的执行

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        // TODO 转换：旧RDD => 算子 => 新RDD
//        val rdd1: RDD[Int] = rdd.map((i: Int) => { i * 2 })
//        val rdd1: RDD[Int] = rdd.map((i: Int) =>  i * 2 )
//        val rdd1: RDD[Int] = rdd.map((i) =>  i * 2 )
//        val rdd1: RDD[Int] = rdd.map(i =>  i * 2 )
        val rdd1: RDD[Int] = rdd.map(_ * 2 )

        // 读取数据
        // collect方法不会转换RDD，会触发作业的执行
        // 所以将collect这样的方法称之为行动（action）算子
        val res: Array[Int] = rdd1.collect()
        println(res.mkString(","))

        sc.stop()

    }

}
