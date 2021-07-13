package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 9:53
  * Describe:
  */
object SparkSQL04_UDF {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        val dataFrame: DataFrame = spark.read.json("input/user.json")
        dataFrame.createTempView("user")

        spark.udf.register("addName", (x: String) => {"Name: " + x})

        spark.sql("select age, addName(name) as NewName from user").show()

        spark.close()

    }

}
