package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/28 19:59
  * Describe:
  */
object csvDataSource {

  def main(args: Array[String]): Unit = {

    // 初始化SparkSession
    val session: SparkSession = SparkSession
      .builder()
      .appName("jsonDataSource")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
    val csvs: DataFrame = session.read.csv("F:\\codedata\\spark\\csv")

    //    val filtered: DataFrame = jsons.where($"ipNum" <=1000)


    csvs.printSchema()

    csvs.show()

    session.stop()


  }

}
