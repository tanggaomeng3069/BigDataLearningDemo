package com.learning.lihaibo.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 16:22
  * Describe:
  */
object Spark56_Persist1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        val mapRDD: RDD[(Int, Int)] = dataRDD.map((num: Int) => {
            println("map......")
            (num, 1)
        })

        // TODO cache操作是在行动算子执行后，会在血缘关系中增加和缓存相关的依赖
        //  cache操作不会切断血缘，一旦发生错误，可以重新执行
        val cacheRDD: RDD[(Int, Int)] = mapRDD.cache()
        println(cacheRDD.toDebugString)
        println(cacheRDD.collect().mkString(","))
        println(cacheRDD.toDebugString)

        sc.stop()
    }

}
