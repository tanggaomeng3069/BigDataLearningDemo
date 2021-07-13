package com.learning.lihaibo.sql

import com.learning.lihaibo.sql.SparkSQL01_Test.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/11/16 20:06
  * Describe:
  */
object SparkSQL02_Test1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        // builder 构建，创建
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        // 导入隐式转换，这里的spark其实是环境对象的名称
        // 要求这个对象必须是val声明的，不然不允许导入隐式转换
        import spark.implicits._

        val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 20),
            (3, "wangwu", 40)
        ))

        // 报错
//        val df: DataFrame = dataRDD.toDF("id", "name", "age")
//        val ds: Dataset[Row] = df.map((row: Row) => {
//            val id = row(0)
//            val name = row(1)
//            val age = row(2)
//            Row(id, "name : " + name, age)
//        })
//        ds.show()


        // 正确
//        val userRDD: RDD[User] = dataRDD.map {
//            case (id, name, age) => {
//                User(id, name, age)
//            }
//        }
//        val userDS: Dataset[User] = userRDD.toDS()
//        val ds: Dataset[User] = userDS.map(user => {
//            User(user.id, "name:" + user.name, user.age)
//        })
//        ds.show()

        // TODO 使用自定义函数在SQL中完成数据的转换操作
        spark.udf.register("addName", (x: String) => "Name:" + x)

        val df: DataFrame = dataRDD.toDF("id", "name", "age")
        df.createTempView("user")

        spark.sql("select addName(name) from user").show()


        // TODO 释放对象
        spark.stop()
    }

    case class User(id: Int, name: String, age: Int)

}
