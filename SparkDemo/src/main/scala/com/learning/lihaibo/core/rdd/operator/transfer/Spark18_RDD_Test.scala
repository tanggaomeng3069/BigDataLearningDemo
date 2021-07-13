package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark18_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark18_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  将List(List(1,2),3,List(4,5))进行扁平化操作

        val dataRDD: RDD[Any] = sc.makeRDD(List(List(1,2),3,List(4,5)))

        val rdd: RDD[Any] = dataRDD.flatMap(data => {
            data match {
                case list: List[_] => list
                case d => List(d)
            }
        })

        println(rdd.collect().mkString(","))


        sc.stop()

    }

}
