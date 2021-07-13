package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/28 20:01
  * Describe:
  */
object paraquetDataSource {

  def main(args: Array[String]): Unit = {

    // 初始化SparkSession
    val session: SparkSession = SparkSession
      .builder()
      .appName("jsonDataSource")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
    val parquets: DataFrame = session.read.parquet("F:\\codedata\\spark\\parquet")

    //    val filtered: DataFrame = jsons.where($"ipNum" <=1000)


    parquets.printSchema()

    parquets.show()

    session.stop()


  }

}
