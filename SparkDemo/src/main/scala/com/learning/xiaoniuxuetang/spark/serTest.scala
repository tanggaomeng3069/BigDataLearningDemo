package com.learning.xiaoniuxuetang.spark

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/27 16:09
  * Describe:
  * 数据格式：test.txt
  * spark
  * hadoop
  * scala
  * python
  * hive
  *
  * /bin/spark-submit --master spark://node-1:7077,node-2:7077 \
  * --class cn.edu360.xiaoniu.spark.serTest \
  * hdfs://manager.bigdata:8020/sertest \
  * hdfs://manager.bigdata:8020/serout
  *
  */
object serTest {

  def main(args: Array[String]): Unit = {

    // 第一种方式
    // 在Driver端被实例化
    // Executor端有多个rules实例
//    val rules = new Rules

    // 第二种方式
    //初始化object（在Driver端）
    //Executor端一个rules实例
    //var rules = Rules
    //println("@@@@@@@@@@@@" + rules.toString + "@@@@@@@@@@@@")

    // 第三种方式
    val conf: SparkConf = new SparkConf().setAppName("serTest")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val r: RDD[(String, String, Double, String)] = lines.map((word: String) => {
      //在map的函数中，创建一个rules实例(太浪费资源)
      //val rules = new Rules
      //函数的执行是在Executor执行的（Task中执行的）
      val hostname: String = InetAddress.getLocalHost.getHostName
      val threadName: String = Thread.currentThread().getName
      //rules的实际是在Executor中使用的
      (hostname, threadName, Rules.rulesMap.getOrElse(word, 0), Rules.toString)
    })

    r.saveAsTextFile(args(1))

    sc.stop()



  }

}
