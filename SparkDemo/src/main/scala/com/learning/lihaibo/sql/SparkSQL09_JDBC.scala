package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 14:52
  * Describe:
  */
object SparkSQL09_JDBC {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        // 读取MySQL数据，需保证数据库，数据表存在
        val df: DataFrame = spark.read
          .format("jdbc")
          .option("url", "jdbc:mysql://managerhd.bigdata:3306/sparkSQL")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "bigdata123")
          .option("dbtable", "user")
          .load()
        df.show()

        // 保存数据，需保证数据库存在，数据表存在不存在无所谓
        df.write
          .format("jdbc")
          .option("url", "jdbc:mysql://managerhd.bigdata:3306/sparkSQL")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "bigdata123")
          .option("dbtable", "user1")
          .mode(SaveMode.Append)
          .save()

        spark.close()
    }

}
