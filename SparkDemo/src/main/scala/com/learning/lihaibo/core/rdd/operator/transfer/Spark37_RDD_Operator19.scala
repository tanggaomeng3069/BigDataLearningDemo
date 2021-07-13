package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark37_RDD_Operator19 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）
        //  RDD默认只能操作单值类型数据
        //  然是通过隐式转换，可以通过扩展操作双值操作

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark37_RDD_Operator19")
        val sc = new SparkContext(sparkConf)

        // TODO reduceByKey: 根据数据的key进行分组，然后对value进行聚合

        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("hello", 1), ("scala", 1), ("hello", 1)
        ))

        // reduceByKey: 第一个参数表示相同的key的value的聚合方式
        //  第二个参数表示聚合后的分区数量
        val rdd1: RDD[(String, Int)] = dataRDD.reduceByKey(_ + _)
        println(rdd1.collect().mkString(","))



        sc.stop()

    }

}
