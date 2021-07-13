package com.learning.xiaoniuxuetang.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

/**
  * Author: tanggaomeng
  * Date: 2020/8/25 14:14
  * Describe: 单机版的ip地址查询省份数据
  */
object MyUtils {

  // ip点分十进制转换为十进制
  def ip2Long(ip: String): Long = {
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  // 读取ip库，返回ip十进制范围及省份信息
  def readRules(path: String): Array[(Long, Long, String)] = {
    // 读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()

    // 对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map((line: String) => {
      val fileds: Array[String] = line.split("[|]")
      val startNum: Long = fileds(2).toLong
      val endNum: Long = fileds(3).toLong
      val province: String = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  // 二分法查找
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high: Int = lines.length - 1

    while (low <= high) {
      val middle: Int = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  def data2MySQL(it: Iterator[(String, Int)]): Unit = {

    // 一个迭代器代表一个分区，分区中有多条数据
    // 在Executor中Task获取一个JDBC连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://10.180.210.232:3306/bigdata?characterEncoding=UTF-8", "root", "bigdata123")

    // 将数据通过Connection写入到数据库
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")

    // 将分区中的数据一条一条写入到MySQL中
    it.foreach((tp: (String, Int)) => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    // 将分区中的数据写完后关闭连接
    if (pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {
    // 数据在内存中
    val rules: Array[(Long, Long, String)] = readRules("G:\\VideoCourse\\小牛学堂大数据1\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\ip.txt")

    // 将ip地址转换为十进制
    val ipNum: Long = ip2Long("114.215.43.42")
    //    println(ipNum)
    // 查找
    val index: Int = binarySearch(rules, ipNum)

    // 根据脚标找到rules对应的数据
    val result: (Long, Long, String) = rules(index)

    val province: String = result._3

    println(province)


  }
}
