package com.learning.lihaibo.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark08_RDD_File_PartitionData2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark08_RDD_File_PartitionData2")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 从磁盘（File）中创建RDD

        // TODO 数据分区
        //  totalsize = 6byte, num = 2
        //  6byte / 2 = 3byte  两个分区，一个分区三个字节
        //  (0, 0+3) => (0, 3)
        //  (3, 3+3) => (3, 6)

        // TODO 数据存储
        //  1@@ => 012
        //  234 => 345

        // TODO 分区存储数据
        //  (0, 0+3) => (0, 3) => 1@@234
        //  (3, 3+3) => (3, 6) =>


//        val fileRDD1: RDD[String] = sc.textFile("input/w3.txt", 2)
//        fileRDD1.saveAsTextFile("output1")

        // TODO hadoop分区是以文件为单位进行划分的
        //  读取数据不能跨越文件
        //  12byte / 2 = 6byte
        //  (0, 6)
        //  (0, 6)
//        val fileRDD2: RDD[String] = sc.textFile("input1", 2)
//        fileRDD2.saveAsTextFile("output2")

        // 12byte / 3 = 4byte
        // 1.txt
        // 1@@ => 012
        // 234 => 345

        // 2.txt
        // 5@@ => 012
        // 678 => 345

        // 1.txt
        // (0, 0+4) => 1@@234
        // (4, 4+2) =>

        // 2.txt
        // (0, 0+4) => 5@@678
        // (4, 4+2) =>



        val fileRDD3: RDD[String] = sc.textFile("input1", 3)
        fileRDD3.saveAsTextFile("output3")


        sc.stop()

    }

}
