package com.learning.xiaoniuxuetang.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/21 10:25
  * Describe: 求每个学科最受欢迎的老师
  */
object subjectFavTeacher1 {

  def main(args: Array[String]): Unit = {

    // topN当参数传入
    val topN: Int = args(1).toInt

    // 初始化
    val conf: SparkConf = new SparkConf().setAppName("subjectFavTeacher1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines: RDD[String] = sc.textFile("hdfs://managerhd.bigdata:8020/learning/spark/teacher.log")

    // 整理数据
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map((lines: String) => {
      val index: Int = lines.lastIndexOf("/")
      val teacher: String = lines.substring(index + 1)
      val httpHost: String = lines.substring(0, index)
      val subject: String = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    // 聚合数据，将学科和老师联合作为key
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey((_: Int) + (_: Int))

    // 分组排序（按学科进行分组）

    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy((_: ((String, String), Int))._1._1)

    // 经过分组，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    // 将每一个组拿出来进行操作
    // 为什么可以调用scala的sortby方法呢？因为一个学科的数据已经在一台机器上的一个scala集合里面了
    // 如果数据量特别大，调用scala的排序，会把数据写入内存，导致内存溢出
    // scala的集合排序是在内存中进行的，但是内存有可能不够用
    // 如果数据量特别大，不建议这样使用
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues((_: Iterable[((String, String), Int)]).toList.sortBy((_: ((String, String), Int))._2).reverse.take(3))

    // 收集结果
    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(result.toBuffer)

    sc.stop()

  }

}
