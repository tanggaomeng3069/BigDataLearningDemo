package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 10:01
  * Describe:
  */
object SparkSQL05_UDAF {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        val dataFrame: DataFrame = spark.read.json("input/user.json")
        dataFrame.createTempView("user")

        spark.udf.register("ageAvg", new MyAvgUDAF)

        spark.sql("select ageAvg(age) as avgAge from user").show()

        // 关闭spark幻境
        spark.close()

    }

    /**
      * 自定义聚合函数类：计算年龄的平均值
      * 1.继承UserDefinedAggregateFunction
      * 2.重写方法（8）
      */
    class MyAvgUDAF extends UserDefinedAggregateFunction{

        // 输入数据的结构
        override def inputSchema: StructType = {
            StructType(
                Array(
                    StructField("age", LongType)
                )
            )
        }

        // 缓冲区数据结构：Buffer
        override def bufferSchema: StructType = {
            StructType(
                Array(
                    StructField("total", LongType),
                    StructField("count", LongType)
                )
            )
        }

        // 函数计算结果的数据类型
        override def dataType: DataType = LongType

        // 函数的稳定性
        override def deterministic: Boolean = true

        // 缓冲区初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
//            buffer(0) = 0L
//            buffer(1) = 0L

            buffer.update(0, 0L)
            buffer.update(1, 0L)
        }

        // 根据输入的值更新缓冲区
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) + 1)
        }

        // 缓冲区数据合并
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        // 计算平均数
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0)/buffer.getLong(1)
        }
    }

}
