package com.learning.lihaibo.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark07_RDD_File_PartitionData1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark07_RDD_File_PartitionData1")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 从磁盘（File）中创建RDD

        // TODO 1.分几个区？
        //  10byte / 4 = 2byte...2byte => 5个分区
        //  0 => (0, 2)
        //  1 => (2, 4)
        //  2 => (4, 6)
        //  3 => (6, 8)
        //  4 => (8, 10)

        // TODO 2.数据如何存储？
        //  数据是以行的方式读取，但是会考虑偏移量（数据的offset）设置
        //  1@@ => 012     @@代表回车换行
        //  2@@ => 345
        //  3@@ => 678
        //  4   => 9

        // TODO 分区对应的数据，按行读取，读取的时候会把整行读取
        //  0 => (0, 2)  => 1
        //  1 => (2, 4)  => 2
        //  2 => (4, 6)  => 3
        //  3 => (6, 8)  =>
        //  4 => (8, 10) => 4


        val fileRDD1: RDD[String] = sc.textFile("input/w.txt", 4)
        fileRDD1.saveAsTextFile("output1")

        sc.stop()

    }

}
