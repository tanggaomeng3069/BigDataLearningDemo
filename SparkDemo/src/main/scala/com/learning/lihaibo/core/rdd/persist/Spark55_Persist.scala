package com.learning.lihaibo.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 16:22
  * Describe:
  */
object Spark55_Persist {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        val mapRDD: RDD[(Int, Int)] = dataRDD.map((num: Int) => {
            println("map......")
            (num, 1)
        })

        // TODO 将计算结果进行缓存，重复使用，提高效率，缓存与不缓存执行对比结果
        //  默认的缓存是存储在Executor端的内存中，数据量大的时候该怎么办？
        //  缓存cache底层其实调用的persist方法
        //  persist方法在持久化数据时会采用不同的存储级别对数据进行持久化操作
        //  cache缓存的默认操作就是将数据保存到内存中
        //  cache存储的数据在内存中，如果内存不够用，Executor可以将内存中的数据进行整理，然后可以丢弃数据。
        //  如果由于Executor端整理内存导致缓存的数据丢失，那么数据操作依然要重头执行，
        //  如果cache后的数据重头执行数据操作的话，那么必须要遵循血缘关系，所以cache操作不能删除血缘关系。
        mapRDD.cache()
        // TODO collect
        println(mapRDD.collect().mkString(","))
        // TODO Save
        mapRDD.saveAsTextFile("output")


        // TODO 或者使用下面得操作
//        val cacheRDD: RDD[(Int, Int)] = mapRDD.cache()
//        println(cacheRDD.collect().mkString(","))
//        cacheRDD.saveAsTextFile("output")

        sc.stop()
    }

}
