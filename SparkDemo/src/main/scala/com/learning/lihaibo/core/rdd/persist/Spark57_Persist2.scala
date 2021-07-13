package com.learning.lihaibo.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 16:22
  * Describe:
  */
object Spark57_Persist2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

        // 一般保存到分布式文件系统中，但是此次示例，保存到本地
        sc.setCheckpointDir("cp")

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        val mapRDD: RDD[(Int, Int)] = dataRDD.map((num: Int) => {
            println("map......")
            (num, 1)
        })

        // TODO 将比较耗时，比较重要的数据，一般会保存到分布式文件系统中。
        //  使用checkpoint方法将数据保存到文件中
        //  执行checkpoint方法前应该先设定检查点的保存目录
//        mapRDD.checkpoint()
//        println(mapRDD.collect().mkString(","))
//        println("*"*100)
//        println(mapRDD.collect().mkString("&"))

        // TODO 检查点的操作中为了保证数据的准确性，在执行时，会启动新的Job
        //  为了提高性能，检查点操作一般会和cache联合使用
        val cacheRDD: RDD[(Int, Int)] = mapRDD.cache()
        cacheRDD.checkpoint()
        println(mapRDD.collect().mkString(","))
        println("*"*100)
        println(mapRDD.collect().mkString("&"))
        println(mapRDD.collect().mkString("*"))


        sc.stop()
    }

}
