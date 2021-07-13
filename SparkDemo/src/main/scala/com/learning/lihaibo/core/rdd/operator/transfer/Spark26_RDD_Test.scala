package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark26_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark26_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  从服务器日志数据apache.log中获取2015年5月17日的请求路径

        val fileRDD: RDD[String] = sc.textFile("input/apache.log")

        val timeRDD: RDD[String] = fileRDD.map(line => {
            val datas: Array[String] = line.split(" ")
            datas(3)
        })

        val filterRDD: RDD[String] = timeRDD.filter(time => {
            val ymr = time.substring(0, 10)
            ymr == "17/05/2015"
        })

        filterRDD.collect().foreach(println)


        sc.stop()

    }

}
