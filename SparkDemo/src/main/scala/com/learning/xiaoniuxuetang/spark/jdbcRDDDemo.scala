package com.learning.xiaoniuxuetang.spark

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/26 15:36
  * Describe:
  */
object jdbcRDDDemo {

  val getConn: () => Connection = () => {
    DriverManager.getConnection("jdbc:mysql://10.180.210.232:3306/bigdata?characterEncoding=UTF-8", "root", "bigdata123")
  }


  def main(args: Array[String]): Unit = {

    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName("jdbcRDDDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 创建RDD，这个RDD会记录以后从MySQL中读取数据
    // new了RDD，里面没有真正的数据，而是告诉RDD，以后出发Action时去哪里读取数据
    val jdbcRDD: JdbcRDD[(String, Int)] = new JdbcRDD(
      sc,
      getConn,
      "select * from access_log where ipNum >= ? and ipNum <= ?",
      1,
      2000,
      2,
      (rs: ResultSet) => {
        val province: String = rs.getString(1)
        val ipNum: Int = rs.getInt(2)
        (province, ipNum)
      }
    )

    // 出发Action
    val result: Array[(String, Int)] = jdbcRDD.collect()

    println(result.toBuffer)
    
    sc.stop()

  }

}
