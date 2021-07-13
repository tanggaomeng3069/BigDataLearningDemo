package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark24_RDD_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark24_RDD_Test")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - RDD 算子（方法）
        //  从服务器日志数据apache.log中获取每个时间段访问量
        
        val fileRDD: RDD[String] = sc.textFile("input/apache.log")

        val timeRDD: RDD[String] = fileRDD.map(line => {
            val datas: Array[String] = line.split(" ")
            datas(3)
        })

        val hourRDD: RDD[(String, Iterable[String])] = timeRDD.groupBy(time => {
            time.substring(11, 13)
        })

        val countRDD: RDD[(String, Int)] = hourRDD.map {
            case (str, iter) => {
                (str, iter.size)
            }
        }
        println(countRDD.collect().mkString(","))

        sc.stop()

    }

}
