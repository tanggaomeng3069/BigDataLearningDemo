package com.learning.lihaibo.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 10:14
  * Describe:
  */
object Spark03_RDD_Memory_Par {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_RDD_Memory_Par")
        val sc = new SparkContext(sparkConf)

        // TODO Spark从内存中创建RDD
        // RDD中分区的数量就是并行度，设定并行度，其实就是设定分区（在资源CPU核充足的情况下）
        // 1.makeRDD的第一个参数：数据源
        // 2.makeRDD的第二个参数：默认并行度（分区的数量）
        //                                    parallelize
        //            numSlices: Int = defaultParallelism

        // scheduler.conf.getInt("spark.default.parallelism", totalCores)
        // 并行度默认会从spark配置信息中获取：spark.default.parallelism
        // 如果从配置信息中获取不到指定参数，会采用默认值：totalCores（总核数）
        // 总核数 = 当前环境中可用核数
        // local => 单核（单线程） => 1
        // local[4] => 4核（4线程） => 4
        // local[*] => 最大核数 => 程序当前运行机器的总核数

        // 优先级
        // 指定分区数 < spark.default.parallelism < local[4] < totalCores
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)
//        println(rdd.collect().mkString("-"))

        // 将RDD处理后的数据保存在分区文件中，该目录不能存在
        rdd.saveAsTextFile("output")

        sc.stop()

    }

}
