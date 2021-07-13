package com.learning.lihaibo.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/12 15:45
  * Describe:
  */
object Spark61_Acc1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO 声明累加器变量
        val sum: LongAccumulator = sc.longAccumulator("sum")

        dataRDD.foreach((num: Int) => {
            // TODO 使用累加器
            sum.add(num)
        })

        // TODO 获取累加器的结果
        println("结果为 = " + sum.value)


        sc.stop()

    }

}
