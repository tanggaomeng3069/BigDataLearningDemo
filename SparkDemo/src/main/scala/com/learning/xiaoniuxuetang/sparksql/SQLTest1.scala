package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Author: tanggaomeng
  * Date: 2020/8/27 20:22
  * Describe:
  */
object SQLTest1 {

  def main(args: Array[String]): Unit = {

    // Spark2.x SQL的编程API（SparkSession）
    // 是Spark2.x SQL执行的入口
    val session: SparkSession = SparkSession
      .builder()
      .appName("SQLTest1")
      .master("local[*]")
      .getOrCreate()

    // 创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://managerhd.bigdata:8020/persion")

    // 将数据进行整理
    val rowRDD: RDD[Row] = lines.map((line: String) => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Row(id, name, age, fv)
    })

    // 结果类型，其实就是表头，用于描述DataFrame
    val schema: StructType = StructType(List(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("fv", DoubleType)
    ))

    // 创建DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD, schema)

    import session.implicits._
    val df1: Dataset[Row] = df.where($"fv" < 100).orderBy($"fv" desc, $"age" asc)

    df1.show()

    session.stop()


  }

}
