package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark23_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark23_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组
        
        val dataRDD: RDD[String] = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"), 2)

        val groupbyRDD: RDD[(Char, Iterable[String])] = dataRDD.groupBy(word => {
            //word.substring(0, 1)
            //word.charAt(0)
            // 隐式转换
            word(0)
        })
        println(groupbyRDD.collect().mkString(","))



        sc.stop()

    }

}
