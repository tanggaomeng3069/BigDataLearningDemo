package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/28 19:54
  * Describe:
  */
object jsonDataSource {

  def main(args: Array[String]): Unit = {

    // 初始化SparkSession
    val session: SparkSession = SparkSession
      .builder()
      .appName("jsonDataSource")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
    val jsons: DataFrame = session.read.json("F:\\codedata\\spark\\json")

//    val filtered: DataFrame = jsons.where($"ipNum" <=1000)


    jsons.printSchema()

    jsons.show()

    session.stop()

  }

}
