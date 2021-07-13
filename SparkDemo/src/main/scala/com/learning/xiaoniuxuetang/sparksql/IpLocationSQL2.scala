package com.learning.xiaoniuxuetang.sparksql

import com.learning.xiaoniuxuetang.spark.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/28 11:28
  * Describe: join的代价太昂贵，而且非常慢，解决思路是将表缓存起来（广播变量）
  */
object IpLocationSQL2 {

  def main(args: Array[String]): Unit = {

    // 初始化SparkSession
    val session: SparkSession = SparkSession.builder()
      .appName("IpLocationSQL2")
      .master("local[*]")
      .getOrCreate()

    // 导入隐式转换
    import session.implicits._
    val ipRulesLines: Dataset[String] = session.read.textFile("hdfs://managerhd.bigdata:8020/learning/ip/ip.txt")
    // 整理数据
    val ipRulesDS: Dataset[(Long, Long, String)] = ipRulesLines.map((line: String) => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    })
    // 收集ip规则到Driver端
    val ipRulesInDriver: Array[(Long, Long, String)] = ipRulesDS.collect()
    // 广播（必须使用SparkContext）
    // 将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = session.sparkContext.broadcast(ipRulesInDriver)

    // 读取日志信息
    val logLines: Dataset[String] = session.read.textFile("hdfs://managerhd.bigdata:8020/learning/access/access.log")
    // 整理数据
    val ipDataFrame: DataFrame = logLines.map((logline: String) => {
      val fields: Array[String] = logline.split("[|]")
      val ip: String = fields(1)
      // ip转化为十进制
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    // 创建视图
    ipDataFrame.createTempView("v_log")

    // 定义一个自定义函数（UDF），并注册
    // 该函数的功能是，输入一个ip对应的的十进制，返回一个省份名称
    session.udf.register("ip2Province", (ipNum: Long) => {
      // 查找ip规则，事先已经广播，已经在Executor中了
      // 函数的逻辑是在Executor中执行的，怎么获取ip规则对应的数据呢？
      // 使用广播变量的引用就可以获得
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 根据ip地址对一个的十进制查找所属省份
      val index: Int = MyUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if (index != -1){
        province = ipRulesInExecutor(index)._3
      }
      province
    })

    // 执行SQL
    val result: DataFrame = session.sql("SELECT ip2Province(ip_num) province, COUNT(*) counts FROM v_log GROUP BY province ORDER BY counts DESC")

    result.show()

    session.stop()


  }

}
