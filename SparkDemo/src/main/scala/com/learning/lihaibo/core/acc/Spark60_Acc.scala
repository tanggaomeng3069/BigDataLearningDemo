package com.learning.lihaibo.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/12 15:45
  * Describe:
  */
object Spark60_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("a", 4)
        ))

        // 因为 sum是在Driver端声明，执行求和是在Executor端执行，所以sum结果为0
        var sum = 0

        dataRDD.foreach{
            case (word, count) => {
                sum = sum + count
                print(sum + "\n")
            }
        }

        println("(a, " + sum + ")")


        sc.stop()

    }

}
