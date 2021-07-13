package com.learning.lihaibo.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 16:22
  * Describe:
  */
object Spark54_Dep {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

        // TODO new ParallelCollectionRDD
        val dataRDD: RDD[String] = sc.makeRDD(List(
            "hello scala", "hello world"
        ))
        println(dataRDD.toDebugString)
        println("*"*80)

        // TODO new MapPartitionsRDD -> new ParallelCollectionRDD
        val wordRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
        println(wordRDD.toDebugString)
        println("*"*80)

        // TODO new MapPartitionsRDD - new MapPartitionsRDD
        val mapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
        println(mapRDD.toDebugString)
        println("*"*80)

        // TODO new ShuffledRDD -> new MapPartitionsRDD
        //  如果Spark的计算过程中某一节点计算失败，那么框架会尝试重新计算
        //  Spark既然想要重新计算，那么就需要z知道数据的来源
        //  并且还要知道数据经历了哪些计算
        //  RDD不保存计算的数据，但是会保存元数据信息，也就是RDD之间的依赖关系，计算逻辑
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        println(reduceRDD.toDebugString)
        println("*"*80)

        println(reduceRDD.collect().mkString(","))


        sc.stop()
    }

}
