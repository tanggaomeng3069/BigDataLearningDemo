package com.learning.xiaoniuxuetang.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/25 16:16
  * Describe:
  */
object ipLocation2 {

  def main(args: Array[String]): Unit = {

    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName("ipLocation2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 从HDFS中读取ip规则
    val ipRulesLines: RDD[String] = sc.textFile("hdfs://managerhd.bigdata:8020/learning/ip/ip.txt")
    // 整理ip规则数据
    val ipRulesRDD: RDD[(Long, Long, String)] = ipRulesLines.map((line: String) => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    })
    // 将分散在多个Executor中的部分数据收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()
    // 将Driver端的数据广播到Executor
    // 广播变量的引用（还在Driver端）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)

    // 创建RDD，从HDFS读取访问日志
    val accessLines: RDD[String] = sc.textFile("hdfs://managerhd.bigdata:8020/learning/access/access.log")
    // 整理日志数据
    val provinceAndOne: RDD[(String, Int)] = accessLines.map((logs: String) => {
      // 将log日志每一行进行切分
      val fields: Array[String] = logs.split("[|]")
      val ip: String = fields(1)
      // 将ip转换为十进制
      val ipNum: Long = MyUtils.ip2Long(ip)
      // 进行二分法查找，通过Driver端的引用获取到Executor中的广播变量
      // 该函数中的代码是在Executor中执行，通过广播变量的引用，就可以拿到当前Executor中的规则数据
      // Driver端广播变量的引用是如何跑在Executor中的？
      // Task是在Driver端生成的，广播变量的引用是伴随着Task被发送到Executor中的
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 查找
      var province = "未知"
      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    })

    // 聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey((_: Int)+(_: Int))
    
    // 收集到Driver端
//    val result: Array[(String, Int)] = reduced.collect()
//    println(result.toBuffer)

    // 多个客户端写入MySQL
    reduced.foreachPartition((it: Iterator[(String, Int)]) => MyUtils.data2MySQL(it))

    sc.stop()

  }

}
