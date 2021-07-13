package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark36_RDD_Operator18 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）
        //  RDD默认只能操作单值类型数据
        //  然是通过隐式转换，可以通过扩展操作双值操作

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark36_RDD_Operator18")
        val sc = new SparkContext(sparkConf)

        // TODO 自定义分区器 -- 自己决定数据放置在哪个分区做处理
        //  cba, wnba, nba
        val dataRDD: RDD[(String, String)] = sc.makeRDD(List(
            ("cba", "消息1"), ("cba", "消息2"), ("cba", "消息3"),
            ("nba", "消息4"), ("wnba", "消息5"), ("nba", "消息6")
        ), 1)

        val rdd1: RDD[(String, String)] = dataRDD.partitionBy(new HashPartitioner(3))
        val rdd2: RDD[(String, String)] = rdd1.partitionBy(new HashPartitioner(3))


        sc.stop()

    }

}
