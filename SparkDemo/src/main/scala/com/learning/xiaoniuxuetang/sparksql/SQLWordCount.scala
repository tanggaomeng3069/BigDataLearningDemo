package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/27 20:46
  * Describe:
  */
object SQLWordCount {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val session: SparkSession = SparkSession.builder()
      .appName("SQLWordCount")
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

    // 注册视图
    words.createTempView("v_wc")

    // 执行SQL（Transformation）
    val result: DataFrame = session.sql("select value, count(*) counts from v_wc group by value order by counts desc")

    // 执行Action
    result.show()

    session.stop()

  }

}
