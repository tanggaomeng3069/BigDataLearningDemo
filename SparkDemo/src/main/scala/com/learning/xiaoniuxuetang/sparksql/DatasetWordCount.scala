package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/27 20:54
  * Describe:
  */
object DatasetWordCount {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val session: SparkSession = SparkSession.builder()
      .appName("DatasetWordCount")
      .master("local[*]")
      .getOrCreate()

    // 指定从哪儿读数据，lazy
    // Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    // Dataset只有一列，默认这列叫value
    val lines: Dataset[String] = session.read.textFile("hdfs://managerhd.bigdata:8020/wc")

    // 整理数据，切分压平
    // 导入隐式转换
    import session.implicits._
    val words: Dataset[String] = lines.flatMap((_: String).split(" "))

    // 使用Dataset的API（DSL）
    //导入聚合函数
    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)


    result.show()

    session.stop()

  }

}
