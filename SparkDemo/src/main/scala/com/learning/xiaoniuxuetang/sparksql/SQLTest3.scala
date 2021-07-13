package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/9/7 8:44
  * Describe:
  */
object SQLTest3 {

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

    // 初始化并整理数据1
    val datas1: Dataset[String] = session.createDataset(List("1,1,2,3,usa", "2,4,5,5,jp", "3,7,8,2,china", "4,0,1,0,jp", "5,2,3,10,china", "6,4,5,11,usa", "7,3,4,12,jp", "8,5,6,13,china", "9,6,7,14,usa", "10,7,8,15,jp"))
    val df1: DataFrame = datas1.map((line: String) => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val cid: Int = fields(1).toInt
      val did: Int = fields(2).toInt
      val value: Int = fields(3).toInt
      val nation: String = fields(4)
      (id, cid, did, value, nation)
    }).toDF("id", "cid", "did", "value", "nation")

    // 注册视图
    df1.createTempView("v_df1")

    // 初始化并整理数据2
    val datas2: Dataset[String] = session.createDataset(List("1,zhangsan", "2,lisi", "3,wangwu", "4,zhaoliu", "5,xiaoming", "6,xiaohong", "7,mogutou", "8,meiye", "9,qiangzi"))
    val df2: DataFrame = datas2.map((line: String) => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      (id, name)
    }).toDF("id", "name")

    // 注册视图
    df2.createTempView("v_df2")

    //执行sql
    val result: DataFrame = session.sql("SELECT sum(v) FROM (SELECT v_df1.id, 1 + 2 + v_df1.value AS v FROM v_df1 JOIN v_df2 WHERE v_df1.id = v_df2.id AND v_df1.did = v_df1.cid + 1 AND v_df2.id > 5) test")


    df1.show()

    df2.show()

    result.show()

    result.explain(true)

    session.stop()

  }
}
