package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 19:25
  * Describe:
  */
object SparkSQL12_loadSava1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        spark.sql("select * from json.`D:\\code_java\\BigDataSpark\\input\\user1.json`").show()

        spark.close()

    }

}
