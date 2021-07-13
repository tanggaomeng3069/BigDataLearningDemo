package com.learning.lihaibo.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 15:11
  * Describe:
  */
object Spark52_Serial {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        // TODO Saprk序列化
//        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//        dataRDD.foreach(
//            (num: Int) => {
//                val user = new User()
//                println("age = " + (user.age + num))
//            }
//        )

        // TODO Exception: Task not serializable
        //  给User增加序列化
        //  如果算子中s使用了算子外的对象，那么在执行时，需要保证这个对象能序列化
//        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//        val user = new User1()
//        dataRDD.foreach(
//            (num: Int) => {
//                println("age = " + (user.age + num))
//            }
//        )

//        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//        val user = new User2()
//        dataRDD.foreach(
//            (num: Int) => {
//                println("age = " + (user.age + num))
//            }
//        )

        // TODO Scala 闭包
        //
        val dataRDD: RDD[Int] = sc.makeRDD(List())
        val user = new User()

        // Spark的算子的操作其实都是闭包，所以闭包有可能包含外部的变量
        // 如果包含了外部的变量，那么就一定要保证外部变量可序列化
        // 所以Spark在提交作业之前，应该对闭包的变量进行检测，检测是否能够序列化
        // 将这个操作称为闭包检测
        dataRDD.foreach(
            (num: Int) => {
                println("age = " + (user.age + num))
            }
        )



        sc.stop()

    }

    class User{
        val age: Int = 20
    }

    class User1 extends Serializable {
        val age: Int = 20
    }

    // 样例类zzi自动混入可序列化特质
    case class User2(age: Int = 20)


}
