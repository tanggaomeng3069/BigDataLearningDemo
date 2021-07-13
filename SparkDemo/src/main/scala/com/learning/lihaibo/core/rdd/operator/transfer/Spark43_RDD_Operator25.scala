package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark43_RDD_Operator25 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark43_RDD_Operator25")
        val sc = new SparkContext(sparkConf)

        // TODO join

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("c",6),("a",4),("b",5)))

        // join方法可以将两个rdd中相同的key的value连接在一起
        // join方法性能不是太高，能不用尽量不要使用
        val resultRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        println(resultRDD.collect().mkString(","))

        println("-"*80)

        val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("a",3)))
        val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("a",6),("a",4),("b",5)))

        val resultRDD2: RDD[(String, (Int, Int))] = rdd3.join(rdd4)

        resultRDD2.collect().foreach(println)


        sc.stop()

    }

}
