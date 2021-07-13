package com.learning.lihaibo.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 15:38
  * Describe:
  */
object Spark05_RDD_File_Par {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark05_RDD_File_Par")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 从磁盘（File）中创建RDD

        // textFile 第一个参数表示读取文件的路径
        // textFile 第二个参数表示最小分区数量
        //          默认值为：math.min(defaultParallelism, 2)
        //                  math.min(12, 2) => 2

        // 12,34
//        val fileRDD1: RDD[String] = sc.textFile("input/w.txt")
//        fileRDD1.saveAsTextFile("output1")

        // 1,2,3,4
//        val fileRDD2: RDD[String] = sc.textFile("input/w.txt", 1)
//        fileRDD2.saveAsTextFile("output2")

        // X
        val fileRDD3: RDD[String] = sc.textFile("input/w.txt", 4)
        fileRDD3.saveAsTextFile("output3")

        // X
        val fileRDD4: RDD[String] = sc.textFile("input/w.txt", 3)
        fileRDD4.saveAsTextFile("output4")


        sc.stop()

    }

}
