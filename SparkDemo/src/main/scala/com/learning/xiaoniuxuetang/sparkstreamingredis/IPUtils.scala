package com.learning.xiaoniuxuetang.sparkstreamingredis

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

/**
  * Author: tanggaomeng
  * Date: 2020/9/24 14:11
  * Describe:
  */
object IPUtils {

  def broadcastIpRules(ssc: StreamingContext, ipRulesPath: String): Broadcast[Array[(Long, Long, String)]] = {

    // 现在获取SparkContext
    val sc: SparkContext = ssc.sparkContext
    val rulesLines: RDD[String] = sc.textFile(ipRulesPath)

    // 整理ip规则数据
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map((line: String) => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    })

    // 将分散在多个Executor中的部分ip规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()

    // 将Driver端的数据广播到Executor
    // 广播变量的引用，还在Driver端
    sc.broadcast(rulesInDriver)

  }

}
