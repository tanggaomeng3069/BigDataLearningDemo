package com.learning.lihaibo.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 13:37
  * Describe:
  */
object Spark50_Operator_Action4 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 算子 - 行动
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        dataRDD.saveAsTextFile("output")
        dataRDD.saveAsObjectFile("output1")
        dataRDD.map((_,1)).saveAsSequenceFile("output2")


        sc.stop()

    }

}
