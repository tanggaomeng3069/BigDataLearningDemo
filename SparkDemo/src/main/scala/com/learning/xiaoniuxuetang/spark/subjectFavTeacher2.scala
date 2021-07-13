package com.learning.xiaoniuxuetang.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/24 15:37
  * Describe:
  */
object subjectFavTeacher2 {

  def main(args: Array[String]): Unit = {

    val subject = Array("bigdata", "javaee", "php")

    // 配置
    val conf: SparkConf = new SparkConf().setAppName("subjectFavTeacher2").setMaster("local[4]")
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

    // 聚合
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey((_: Int) + (_: Int))

    // cache到内存（标记为Cache的RDD以后被反复使用时，才放入内存）
    val cached: RDD[((String, String), Int)] = reduced.cache()


    // scala的集合排序是在内存中进行的，如果数据量特别大，有可能导致内存不够用，内存溢出
    // 可以使用RDD的sortby方法，内存+磁盘的方式进行排序

    for (subje <- subject){
      // 该RDD中对应的数据仅有一个学科的数据（因为过滤了）
      val filtered: RDD[((String, String), Int)] = cached.filter((_: ((String, String), Int))._1._1 == subje)

      val result: Array[((String, String), Int)] = filtered.sortBy((_: ((String, String), Int))._2, ascending = false).take(3)

      println(result.toBuffer)
    }

    // 前面cache的数据已经计算完了，后面还有很多其他的指标要计算
    // 后面的计算指标也要出发很多Action，最好将数据缓存到内存中
    // 原来的数据占用内存，把原来的数据释放掉，才能缓存新的数据
    //cached.unpersist(true)



    sc.stop()
  }
}
