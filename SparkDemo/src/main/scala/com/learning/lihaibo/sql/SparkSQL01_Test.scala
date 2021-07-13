package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/11/16 20:06
  * Describe:
  */
object SparkSQL01_Test {

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

        // TODO 逻辑操作
        val jsonDF: DataFrame = spark.read.json("input/user.json")

        // TODO SQL
        //  将DF转换为临时视图
        jsonDF.createOrReplaceTempView("user")
        spark.sql("select * from user").show()

        // TODO DSL
        //  如果查询列命需要使用单引号，那么需要使用隐式转换
        jsonDF.select("name", "age").show()
        jsonDF.select('name, 'age).show()


        val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 20),
            (3, "wangwu", 40)
        ))

        // TODO RDD <=> DataFrame
        val df: DataFrame = dataRDD.toDF("id", "name", "age")
        val dfToRDD: RDD[Row] = df.rdd

        // TODO RDD <=> DataSet
        val userRDD: RDD[User] = dataRDD.map {
            case (id, name, age) => {
                User(id, name, age)
            }
        }
        val userDS: Dataset[User] = userRDD.toDS()
        val dsToRDD: RDD[User] = userDS.rdd

        // TODO DataFrame <=> DataSet
        val dfToDS: Dataset[User] = df.as[User]
        val dfToDF: DataFrame = dfToDS.toDF()


        dataRDD.foreach(println)
        df.show()
        userDS.show()

        // TODO 释放对象
        spark.stop()
    }
    case class User(id: Int, name: String, age: Int)

}
