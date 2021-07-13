package com.learning.lihaibo.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 16:22
  * Describe:
  */
object Spark58_Persist3 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

        // 一般保存到分布式文件系统中，但是此次示例，保存到本地
        sc.setCheckpointDir("cp")

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        val mapRDD: RDD[(Int, Int)] = dataRDD.map((num: Int) => {
            (num, 1)
        })

        // TODO 检查点操作会切断血缘关系，一旦数据丢失不会重头读取数据。
        //  因为检查点会将数据保存到分布式存储系统中，数据相对来说比较安全，不容易丢失。
        //  所以会切断血缘，等同于产生了新的数据源。
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)
        println(mapRDD.collect().mkString(","))
        println(mapRDD.toDebugString)



        sc.stop()
    }

}
