package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark32_RDD_Operator14 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark32_RDD_Operator14")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,4,2,6,5,0))

        // TODO sortBy
        //  默认排序规则是升序
        val sortByRDD: RDD[Int] = rdd.sortBy(num => num)
        println(sortByRDD.collect().mkString(","))

        //  sortBy可以通过传递第二个参数改变排序规则
        val sortByRDD1: RDD[Int] = rdd.sortBy(num => num, false)
        println(sortByRDD1.collect().mkString(","))

        // sortBy可以传递第三个参数改变分区



        sc.stop()

    }

}
