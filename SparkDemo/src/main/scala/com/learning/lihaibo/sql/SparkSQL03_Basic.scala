package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 8:42
  * Describe:
  */
object SparkSQL03_Basic {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行幻境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        // TODO 执行逻辑操作

        // TODO DataFrame
//        val df: DataFrame = spark.read.json("input/user.json")
//        df.show()

        // TODO DataFrame => SQL
//        df.createTempView("people")
//        spark.sql("select * from people").show()
//        spark.sql("select name, age from people").show()
//        spark.sql("select avg(age) from people").show()
//        // 报错
//        spark.sql("select avg(age),name from people").show()

        // TODO DataFrame => DSL
        //  在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
//        df.select("name","age").show()
//        df.select($"age" + 1 ).show()
//        df.select('age + 1).show()

        // TODO DataSet
        //  DataFrame其实是特定泛型的DataSet
//        val seq: Seq[Int] = Seq(1,2,3,4)
//        val ds: Dataset[Int] = seq.toDS()
//        ds.show()


        val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 40),
            (3, "wangwu", 50)
        ))
        // TODO RDD <=> DataFrame
        val df: DataFrame = dataRDD.toDF("id", "name", "age")
//        df.show()
//        val dfToRDD: RDD[Row] = df.rdd
//        dfToRDD.foreach(println)
//        println(dfToRDD.collect().mkString(","))

        // TODO DataFrame <=> DataSet
//        val dfToDS: Dataset[User] = df.as[User]
//        dfToDS.show()
//        val dsToDF: DataFrame = dfToDS.toDF()
//        dsToDF.show()

        // TODO RDD <=> DataSet
        val rddToDS: Dataset[User] = dataRDD.map {
            case (id, name, age) => {
                User(id, name, age)
            }
        }.toDS()
        rddToDS.show()

        val dsToRDD: RDD[User] = rddToDS.rdd
        println(dsToRDD.collect().mkString(","))

        // TODO 关闭幻境
        spark.close()

    }

    case class User(id: Int, name: String, age: Int)

}
