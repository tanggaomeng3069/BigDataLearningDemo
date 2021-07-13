package com.learning.xiaoniuxuetang.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Author: tanggaomeng
  * Date: 2020/8/25 20:54
  * Describe:
  */
object customSort5 {

  def main(args: Array[String]): Unit = {

    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName("customSort5").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users = Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    // 将Driver端的数据并行化变成RDD
    val lines: RDD[String] = sc.parallelize(users)

    // 切分整理数据
    val tpRDD: RDD[(String, Int, Int)] = lines.map((line: String) => {
      val fields: Array[String] = line.split(" ")
      val name: String = fields(0)
      val age: Int = fields(1).toInt
      val fv: Int = fields(2).toInt
      (name, age, fv)
    })

    // 充分利用元组的比较规则，元组的比较规则：先比第一，相等再比第二
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy((tp: (String, Int, Int)) => (-tp._3, tp._2))

    val result: Array[(String, Int, Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()

  }

}
