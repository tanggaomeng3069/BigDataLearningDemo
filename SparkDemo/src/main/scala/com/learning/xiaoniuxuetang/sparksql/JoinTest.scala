package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/28 9:44
  * Describe:
  */
object JoinTest {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val session: SparkSession = SparkSession
      .builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    // 导入隐式转换
    import session.implicits._
    // 读取数据
    val lines: Dataset[String] = session.createDataset(List("1,laozhoa,china", "2,laoduan,usa", "3,laoyang,jp"))
    // 整理数据
    val tpDs: Dataset[(Long, String, String)] = lines.map((line: String) => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val nation: String = fields(2)
      (id, name, nation)
    })
    // Dataset转换为DataFrame
    val df1: DataFrame = tpDs.toDF("id", "name", "nation")

    // 读取数据
    val nations: Dataset[String] = session.createDataset(List("china,中国", "usa,美国"))
    // 整理数据
    val nDs: Dataset[(String, String)] = nations.map((lin: String) => {
      val fields: Array[String] = lin.split(",")
      val ename: String = fields(0)
      val cname: String = fields(1)
      (ename, cname)
    })
    // Dataset转换为DataFrame
    val df2: DataFrame = nDs.toDF("ename", "cname")

    // 第一种方式，创建视图
    //df1.createTempView("v_users")
    //df2.createTempView("v_nations")
    //val result: DataFrame = session.sql("select name, cname from v_users join v_nations on nation=ename")


    // 第二种方式
    val result: DataFrame = df1.join(df2, $"nation" === $"ename", "left_outer")

    result.show()

    session.stop()

  }

}
