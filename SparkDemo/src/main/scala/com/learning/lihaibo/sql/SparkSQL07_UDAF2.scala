package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, _}

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 10:01
  * Describe:
  */
object SparkSQL07_UDAF2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        val dataFrame: DataFrame = spark.read.json("input/user.json")

        // 在早期的版本中，spark不能在sql中使用强类型UDAF操作
        // 早期的UDAF强制类型聚合函数使用DSL语法操作
        val ds: Dataset[User] = dataFrame.as[User]

        // 将UDAF函数转换为查询的对象
        val udafColumn: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

        ds.select(udafColumn).show()

        // 关闭spark幻境
        spark.close()

    }

    case class User(name: String, age: Long)
    case class Buff(var total: Long, var count: Long)
    /**
      * 自定义聚合函数类：计算年龄的平均值
      * 1.继承org.apache.spark.sql.expressions.Aggregator，定义泛型
      *     IN：输入的数据类型，User
      *     BUF：缓冲区的数据类型：Buff
      *     OUT：输出的数据类型：Long
      * 2.重写方法（6）
      */
    class MyAvgUDAF extends Aggregator[User, Buff, Long] {

        // z & zero : 初始值或者 0值
        // 缓冲区的初始化
        override def zero: Buff = {
            Buff(0L, 0L)
        }

        // 根据输入的数据更新缓冲区中的数据
        override def reduce(buffer: Buff, in: User): Buff = {
            buffer.total = buffer.total + in.age
            buffer.count = buffer.count + 1
            buffer
        }

        // 合并缓冲区
        override def merge(buffer1: Buff, buffer2: Buff): Buff = {
            buffer1.total = buffer1.total + buffer2.total
            buffer1.count = buffer1.count + buffer2.count
            buffer1
        }

        // 计算结果
        override def finish(buff: Buff): Long = {
            buff.total/buff.count
        }

        // 缓冲区的编码操作
        override def bufferEncoder: Encoder[Buff] = Encoders.product

        // 输出的编码操作
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }

}
