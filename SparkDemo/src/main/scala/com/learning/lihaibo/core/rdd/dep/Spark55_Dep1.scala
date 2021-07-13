package com.learning.lihaibo.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 16:22
  * Describe:
  */
object Spark55_Dep1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[String] = sc.makeRDD(List(
            "hello scala", "hello world"
        ))
        // TODO OneToOneDependency (1:1)
        //  依赖关系中，现在的分区和之前的分区一一对应
        println(dataRDD.dependencies)
        println("-"*80)

        val wordRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
        // TODO OneToOneDependency (1:1)
        println(wordRDD.dependencies)
        println("-"*80)

        val mapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
        // TODO OneToOneDependency (1:1)
        println(mapRDD.dependencies)
        println("-"*80)

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        // TODO ShuffleDependency (N:N)
        println(reduceRDD.dependencies)
        println("-"*80)

        println(reduceRDD.collect().mkString(","))


        sc.stop()
    }

}
