package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 14:44
  * Describe:
  */
object SparkSQL08_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        val dataFrame: DataFrame = spark.read.json("input/user.json")

        val ds: Dataset[User] = dataFrame.as[User]
        ds.show()


        spark.close()
    }

    case class User(name: String, age: Long)

}
