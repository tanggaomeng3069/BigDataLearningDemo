package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/13 14:42
  * Describe:
  */
object Spark27_RDDTest {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
        val sc = new SparkContext(sparkConf)

        val dataRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
        val dataRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("c",2),("c",3)))

        val cogroupRDD1: RDD[(String, (Iterable[Int], Iterable[Int]))] = dataRDD1.cogroup(dataRDD2)
        cogroupRDD1.foreach(println)

        println("*"*100)

        val cogroupRDD2: RDD[(String, (Iterable[Int], Iterable[Int]))] = dataRDD2.cogroup(dataRDD1)
        cogroupRDD2.foreach(println)


        sc.stop()
    }

}
