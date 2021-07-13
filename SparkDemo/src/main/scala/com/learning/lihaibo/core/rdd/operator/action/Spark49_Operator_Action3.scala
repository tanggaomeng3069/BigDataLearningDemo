package com.learning.lihaibo.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 13:37
  * Describe:
  */
object Spark49_Operator_Action3 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 算子 - 行动
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // TODO sum
        val sum: Double = dataRDD.sum()
        println(sum)

        // TODO aggregate
        //  aggregateByKey：初始值只参与了分区内的计算
        //  aggregate：初始值在内区内参与了计算，同时分区间计算爷参与计算
        val result: Int = dataRDD.aggregate(0)(_+_, _+_)

        println(result)

        // TODO fold
        val folds: Int = dataRDD.fold(0)(_+_)
        println(folds)

        // TODO countByKey
        val dataRDD1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 1), ("a", 1)
        ))
        val wordToCount: collection.Map[String, Long] = dataRDD1.countByKey()
        println(wordToCount)

        // TODO
        val dataRDD2: RDD[String] = sc.makeRDD(List(
            "a", "a", "a", "b", "b", "b"
        ))
        val wordToCount2: collection.Map[String, Long] = dataRDD2.countByValue()
        println(wordToCount2)

        sc.stop()

    }

}
