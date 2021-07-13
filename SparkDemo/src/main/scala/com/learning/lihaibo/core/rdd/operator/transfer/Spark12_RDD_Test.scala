package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark12_RDD_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark12_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  从服务器日志数据apache.log中获取用户请求URL资源路径

        val fileRDD: RDD[String] = sc.textFile("input/apache.log")
        val urlRDD: RDD[String] = fileRDD.map(line => {
            val datas: Array[String] = line.split(" ")
            datas(6)
        })
        urlRDD.collect().foreach(println)

        sc.stop()

    }

}
