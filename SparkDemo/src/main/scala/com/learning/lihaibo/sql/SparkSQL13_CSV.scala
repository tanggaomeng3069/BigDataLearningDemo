package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 19:50
  * Describe:
  */
object SparkSQL13_CSV {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        val df: DataFrame = spark.read
          .format("csv")
          .option("sep", ";")
          .option("inferSechema", "true")
          .option("header", "true")
          .load("input/user2.csv")

        df.show()


        spark.close()

    }

}
