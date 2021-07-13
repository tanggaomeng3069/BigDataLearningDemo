package com.learning.xiaoniuxuetang.sparkstreamingredis

import com.learning.xiaoniuxuetang.spark.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.spark_project.jetty.util.Fields
import redis.clients.jedis.Jedis

/**
  * Author: tanggaomeng
  * Date: 2020/9/24 15:41
  * Describe:
  */
object CalculateUtil {

  /**
    * 计算成交总金额
    * @param fields
    */
  def calculateIncome(fields: RDD[Array[String]]): Unit = {

    // 将计算后的数据写入Redis
    val priceRDD: RDD[Double] = fields.map((arr: Array[String]) => {
      val price: Double = arr(4).toDouble
      price
    })
    // reduce是一个Action，会把结果返回到Driver端
    // 将当前批次的总金额返回
    val sum: Double = priceRDD.reduce((_: Double) + (_: Double))

    // 获取一个jedis连接
    val conn: Jedis = JedisConnectionPool.getConnection()
    // 将历史值和当前值进行累加
    // conn.set(Constant.TOTAL_INCOME, sum.toString) // 只会更新覆盖，不会累加
    conn.incrByFloat(Constant.TOTAL_INCOME, sum)  // 累加
    // 释放连接
    conn.close()

  }

  /**
    * 计算分类的成交金额
    * @param fields
    */
  def calculateItem(fields: RDD[Array[String]]): Unit = {
    // 对field的map方法是在哪一端调用的呢？Driver
    val itemAndParice: RDD[(String, Double)] = fields.map((arr: Array[String]) => {
      // 分类
      val item: String = arr(2)
      // 金额
      val parice: Double = arr(4).toDouble
      (item, parice)
    })

    // 按商品分类进行聚合
    val reduced: RDD[(String, Double)] = itemAndParice.reduceByKey((_: Double) + (_: Double))
    // 将当前批次的数据累加到Redis中
    // foreachPartition是一个Action
    // 现在这种方式，jedis的连接是在哪一端创建的（Driver）
    // 在Driver端获取到jedis连接不好
    // val conn = JedisConnectionPool.getConnection()

    reduced.foreachPartition((part: Iterator[(String, Double)]) => {
      // 获取一个jedis连接
      // 这个连接其实是在Executor中获取的
      // JedisConnectionPool在一个Executor进程中有几个实例（单例）
      val conn: Jedis = JedisConnectionPool.getConnection()
      part.foreach((t: (String, Double)) => {
        // 一个连接更新多条数据
        conn.incrByFloat(t._1, t._2)
      })
      // 将当前分区中的数据更新完毕，关闭连接
      conn.close()
    })

  }

  def calculateZone(fields: RDD[Array[String]], broadcastRef: Broadcast[Array[(Long, Long, String)]]): Unit = {

    val provinceAndPrice: RDD[(String, Double)] = fields.map((arr: Array[String]) => {
      val ip: String = arr(1)
      val price: Double = arr(4).toDouble
      val ipNum: Long = MyUtils.ip2Long(ip)
      // 在Executor中获取到广播的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value
      // 二分法查找
      val index: Int = MyUtils.binarySearch(allRules, ipNum)
      var province = "未知"
      if (index != -1) {
        province = allRules(index)._3
      }
      // 省份，订单金额
      (province, price)
    })
    // 按省份进行聚合
    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey((_: Double) + (_: Double))

    // 将数据更新到redis中
    reduced.foreachPartition((part: Iterator[(String, Double)]) => {
      val conn: Jedis = JedisConnectionPool.getConnection()
      part.foreach((t: (String, Double)) => {
        conn.incrByFloat(t._1, t._2)
      })
      conn.close()
    })

  }

}
