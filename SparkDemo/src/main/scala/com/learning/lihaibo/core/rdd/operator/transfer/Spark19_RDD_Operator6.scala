package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark19_RDD_Operator6 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark19_RDD_Operator6")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  glom => 将每个分区的数据转换为数组

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        val rdd: RDD[Array[Int]] = dataRDD.glom()

        rdd.foreach(arry => {
            println(arry.mkString(","))
        })



        sc.stop()

    }

}
