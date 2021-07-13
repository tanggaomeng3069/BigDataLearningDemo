package com.learning.scalaHanShunPing.chapter01

/**
  * Author: tanggaomeng
  * Date: 2020/11/12 19:11
  * Describe:
  */

/**
  * 只要看到object，应该有如下思考：
  * 1. object Scala001_TestScala 对应一个 Scala001_TestScala$ 的一个静态对象 MODULE$
  * 2. 在程序中是一个单例模式
  */
object Scala001_TestScala {

    def main(args: Array[String]): Unit = {
        println("hello scala, spark....")
        var num1: Int = 10
        var num2: Int = 20
        println(num1 + num2)

    }

}
