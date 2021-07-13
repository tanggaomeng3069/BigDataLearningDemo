package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 19:25
  * Describe:
  */
object SparkSQL11_loadSava {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        // TODO 通用的读取
        //  SparkSQL通用读取的默认数据格式是：Parquet列式存储格式。
        //  val frame: DataFrame = spark.read.load("input/user.parquet")
        //  如果想要改变读取文件的格式，需要使用特殊的操作
        //  val frame: DataFrame = spark.read.format("json").load("input/user.json")

        // TODO 如果读取的文件格式为JSON格式，Spark对JSON文件的格式有要求
        //  JSON => JavaScript Object Notation
        //  JSON文件的格式要求整个文件满足JSON的语法规则
        //  Spark读取文件默认是以行为单位来读取
        //  Spark读取JSON文件时，要求文件中的每一行符合JSON的格式要求

        val df: DataFrame = spark.read.format("json").load("input/user1.json")
        df.show()


        // TODO 通用的保存
        //  SparkSQL默认通用保存格式为Parquet格式
        //  如果想要保存的格式是指定的格式，需要进行格式化操作
        //  如果路径已经存在，那么执行保存操作会发生错误
        //  df.write.format("json").save("output")
        //  如果非得想要在路径已经存在的情况下，保存数据，那么可以使用保存模式
        df.write.mode("append").format("json").save("output")





        spark.close()

    }

}
