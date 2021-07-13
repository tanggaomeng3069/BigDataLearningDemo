package com.learning.xiaoniuxuetang.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/25 15:58
  * Describe:
  */
object ipLocation1 {

  def main(args: Array[String]): Unit = {

    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName("ipLocation1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 在Driver端获取到全部的ip规则数据（全部的ip规则数据在某一台机器上，跟Driver在同一台机器上）
    // 全部的ip规则在Driver端（在Driver端的内存中）
    val rules: Array[(Long, Long, String)] = MyUtils.readRules("G:\\VideoCourse\\小牛学堂大数据1\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\ip.txt")

    // 将Driver端的数据广播到Executor中
    // 调用sc上的广播方法
    // 广播变量的引用还在Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    // 创建RDD读取访问日志
    val accessLines: RDD[String] = sc.textFile("hdfs://managerhd.bigdata:8020/learning/access/access.log")

    // 在Driver端定义一个函数
    val func: String => (String, Int) = (line: String) => {
      val fileds: Array[String] = line.split("[|]")
      val ip: String = fileds(1)
      // 将ip转换为十进制
      val ipNum: Long = MyUtils.ip2Long(ip)
      // 进行二分法查找，通过Driver端的引用获取到Executor中的广播变量
      // 该函数中的代码是在Executor中被调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播规则了
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 查找
      var province = "未知"
      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1){
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    }

    // 整理数据
    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)

    // 聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey((_: Int)+(_: Int))

    // 将结果打印
    val result: Array[(String, Int)] = reduced.collect()

    println(result.toBuffer)

    sc.stop()

  }
}
