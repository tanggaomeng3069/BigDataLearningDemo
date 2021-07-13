package com.learning.xiaoniuxuetang.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author: tanggaomeng
  * Date: 2020/8/25 20:19
  * Describe:
  */
object customSort1 {

  def main(args: Array[String]): Unit = {

    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName("customSort1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users = Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    // 将Driver端的数据并行化编程RDD
    val lines: RDD[String] = sc.parallelize(users)

    // 切分整理数据
    val userRDD: RDD[User] = lines.map((line: String) => {
      val fields: Array[String] = line.split(" ")
      val name: String = fields(0)
      val age: Int = fields(1).toInt
      val fv: Int = fields(2).toInt
      new User(name, age, fv)
    })

    // 将RDD里面装的User类型的数据进行排序
    val sorted: RDD[User] = userRDD.sortBy((u: User) => u)

    val result: Array[User] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}


class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = s"name: $name, age: $age, fv: $fv"
}
