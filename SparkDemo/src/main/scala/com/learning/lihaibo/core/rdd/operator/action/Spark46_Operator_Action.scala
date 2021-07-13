package com.learning.lihaibo.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 13:37
  * Describe:
  */
object Spark46_Operator_Action {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 算子 - 行动
        //  所谓的行动算子，其实不再产生新的RDD，而是触发作业的执行
        //  行动算子执行后，会获取到作业的执行结果
        //  转换算子不会触发作业的执行，只是功能的扩展和包装。
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO Spark的行动算子执行时，会产生Job对象，然后提交这个Job对象
        val result: Array[Int] = dataRDD.collect()

        result.foreach(println)

        sc.stop()

    }

}
