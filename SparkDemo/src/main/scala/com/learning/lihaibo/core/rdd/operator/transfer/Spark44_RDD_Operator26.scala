package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark44_RDD_Operator26 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark44_RDD_Operator26")
        val sc = new SparkContext(sparkConf)

        // TODO leftOuterJoin
        // TODO rightOuterJoin

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",4),("b",5)))

        val resultRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
//        rdd1.rightOuterJoin(rdd2)

        // TODO

        resultRDD.collect().foreach(println)


        sc.stop()

    }

}
