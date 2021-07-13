package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.util.Properties

import scala.collection.JavaConverters._

/**
  * Author: tanggaomeng
  * Date: 2020/8/28 19:17
  * Describe:
  */
object jdbcDataSource {

  def main(args: Array[String]): Unit = {

    // 初始化SparkSession
    val session: SparkSession = SparkSession
      .builder()
      .appName("jdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    // 导入隐式转换
    import session.implicits._

    // load这个方法会读取真正的mysql数据吗？
    // 不会读取真正的数据，但是会连接mysql读取schema信息
    val logs: DataFrame = session.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://managerhd.bigdata:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "access_log",
        "user" -> "root",
        "password" -> "bigdata123")
    ).load()

    // 输出schema信息
//    logs.printSchema()

//    logs.show()

    // 过滤
//    val filtered: Dataset[Row] = logs.filter((r: Row) => {
//      r.getAs[Int]("ipNum") <= 1000
//    })
//    filtered.show()

    // lambda表达式
//    val filtered2: Dataset[Row] = logs.filter($"ipNum" <= 1000)
//    filtered2.show()

    val filtered3: Dataset[Row] = logs.where($"ipNum" <= 1000)
//    filtered3.show()

    val result: DataFrame = filtered3.select($"province", $"ipNum" * 100 as "ipNum")
//    result.show()

//    val props = new Properties()
//    props.put("user", "root")
//    props.put("password", "bigdata123")
//    result.write.mode("ignore").jdbc("jdbc:mysql://managerhd.bigdata:3306/bigdata", "access_log2", props)

    // DataFrame保存成text时出错（只能报错一列）
//    result.write.text("F:\\codedata\\spark\\text")

    result.write.json("F:\\codedata\\spark\\json")

    // 中文乱码,但是session.read.csv读取的时候，显示正常
    result.write.csv("F:\\codedata\\spark\\csv")

    result.write.parquet("F:\\codedata\\spark\\parquet")


    session.stop()

  }
}
