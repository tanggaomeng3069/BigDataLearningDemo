package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import com.learning.xiaoniuxuetang.spark.MyUtils

/**
  * Author: tanggaomeng
  * Date: 2020/8/28 10:00
  * Describe:
  */
object IpLocationSQL {

  def main(args: Array[String]): Unit = {

    // 初始化SparkSession
    val session: SparkSession = SparkSession
      .builder()
      .appName("IpLocationSQL")
      .master("local[*]")
      .getOrCreate()

    // 导入隐式转换
    import session.implicits._
    // 读取ip规则数据
    val ipRulesLines: Dataset[String] = session.read.textFile("hdfs://managerhd.bigdata:8020/learning/ip/ip.txt")
    // 整理数据
    val ipRulesDS: Dataset[(Long, Long, String)] = ipRulesLines.map((line: String) => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    })
    // Dataset转换为DataFrame
    val ipRulesDF: DataFrame = ipRulesDS.toDF("startNum", "endNum", "province")

    // 读取日志信息
    val logLines: Dataset[String] = session.read.textFile("hdfs://managerhd.bigdata:8020/learning/access/access.log")
    // 整理数据
    val ipNumDS: Dataset[Long] = logLines.map((logline: String) => {
      val fields: Array[String] = logline.split("[|]")
      val ip: String = fields(1)
      // ip转化为十进制
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    })
    // Dataset转换为DataFrame
    val ipNumDF: DataFrame = ipNumDS.toDF("ipNum")

    // 创建视图
    ipRulesDF.createTempView("ip_rules")
    ipNumDF.createTempView("ip_num")
    val result: DataFrame = session.sql("select province, count(*) counts from ip_num join ip_rules on (ipNum >= startNum and ipNum <= endNum) group by province order by counts DESC")

    result.show()

    session.stop()



  }

}
