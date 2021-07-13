package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Author: tanggaomeng
  * Date: 2020/8/27 19:53
  * Describe:Spark1接口
  */
object SQLDemo2 {

  def main(args: Array[String]): Unit = {

    // 提交的这个程序可以连接到spark集群
    val conf: SparkConf = new SparkConf().setAppName("SQLDemo2").setMaster("local[*]")
    // 创建Context的连接（程序执行的入口）
    val sc = new SparkContext(conf)

    // SparkContext不能创建特殊的RDD（DataFrame）
    // 将SparkContext包装，进而增强
    val sqlContext = new SQLContext(sc)

    // 创建特殊的RDD（DataFrame），就是有schema信息的RDD
    // 先有一个普通的RDD，然后再关联上schema，进而转化为DataFrame

    val lines: RDD[String] = sc.textFile("hdfs://managerhd.bigdata:8020/persion")
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
    val sch: StructType = StructType(List(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("fv", DoubleType)
    ))

    // 将RowRDD关联schema
    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, sch)

    // 变成DF后就可以使用两种API进行编程了
    // 把DataFrame先注册临时表
    bdf.registerTempTable("t_boy")

    // 书写SQL（SQL方法是一个Transformation）
    val result: DataFrame = sqlContext.sql("select * from t_boy order by fv desc, age asc")

    // 查看结果（触发Action）
    result.show()

    sc.stop()


  }

}
