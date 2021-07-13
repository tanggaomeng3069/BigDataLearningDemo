package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/9/9 14:15
  * Describe:
  */
object SQLTest4 {

  def main(args: Array[String]): Unit = {

    // 初始化SparkSession
    val session: SparkSession = SparkSession
      .builder()
      .appName("SQLTest3")
      .master("local[*]")
      .config("spark.sql.crossJoin.enabled", value = true)
      .getOrCreate()

    // 导入隐式转换
    import session.implicits._
    // 读取数据
    val df1: DataFrame = session.read
      .option("inferSchema", value = true) // 推断数据类型
      .option("delimiter", ",") // 可设置分隔符，默认，
      .option("nullValue", "?") // 设置空值
      .option("header", value = true) // 表示有表头，若没有则为false
      .csv("F:\\codedata\\spark\\sqltest4\\table3.csv")
    df1.createTempView("v_df1")

    val df2: DataFrame = session.read
      .option("inferSchema", value = true) // 推断数据类型
      .option("delimiter", ",") // 可设置分隔符，默认，
      .option("nullValue", "?") // 设置空值
      .option("header", value = true) // 表示有表头，若没有则为false
      .csv("F:/codedata/spark/sqltest4/table4.csv")
    df2.createTempView("v_df2")

    //df1.show()
    //df2.show()
    //执行sql
    val result: DataFrame = session.sql("SELECT sum(v) FROM (SELECT v_df1.id, 1 + 2 + v_df1.value AS v FROM v_df1 JOIN v_df2 WHERE v_df1.id = v_df2.id AND v_df1.did = v_df1.cid + 1 AND v_df2.id > 5) test")

    result.show()

    result.explain(true)

    session.stop()
  }

}
