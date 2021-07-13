package com.learning.xiaoniuxuetang.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/21 9:19
  * Describe: 求最受欢迎的老师前三名
  */
object FavTeacher {


  def main(args: Array[String]): Unit = {

    //初始化
    val conf: SparkConf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines: RDD[String] = sc.textFile("hdfs://managerhd.bigdata:8020/learning/spark/teacher.log")
    val teacherAndOne: RDD[(String, Int)] = lines.map((lines: String) => {
      val index: Int = lines.lastIndexOf("/")
      val teacher: String = lines.substring(index + 1)
      //      val httpHost: String = lines.substring(0, index)
      //      val hostname: String = new URL(httpHost).getHost.split("[.]")(0)
      (teacher, 1)
    })

    // 聚合
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey((_: Int) + (_: Int))

    // 排序
    val sorted: RDD[(String, Int)] = reduced.sortBy((_: (String, Int))._2, ascending = false)

    // 出发action
    //    val result: Array[(String, Int)] = sorted.collect()
    val result: Array[(String, Int)] = sorted.top(3)
    println(result.toBuffer)

    sc.stop()
  }
}
