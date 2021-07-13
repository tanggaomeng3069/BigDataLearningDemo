package com.learning.xiaoniuxuetang.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/17 17:04
  * Describe:
  */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")
    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))

    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = lines.flatMap((_: String).split(" "))
    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_: String, 1))
    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey((_: Int) + (_: Int))
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy((_: (String, Int))._2, ascending = false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}
