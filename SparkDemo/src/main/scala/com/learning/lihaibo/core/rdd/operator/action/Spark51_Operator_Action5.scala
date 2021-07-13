package com.learning.lihaibo.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 13:37
  * Describe:
  */
object Spark51_Operator_Action5 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 算子 - 行动
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO foreach 方法
        //  集合的方法中的代码是在当前的节点（Driver）中执行的
        //  foreach方法是在当前的节点执行
        dataRDD.collect().foreach(println)
        println("*"*20)
        // TODO foreach 算子
        //  rdd的方法称为算子
        //  算子的逻辑代码是在分布式计算节点Executor节点执行
        //  foreach算子可以将循环在不同的计算节点执行
        //  算子之外的代码在Driver端执行
        dataRDD.foreach(println)

        sc.stop()

    }

}
