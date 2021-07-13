package com.learning.lihaibo.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/10 19:41
  * Describe: 偏向于scala方法
  */
object Spark01_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO Spark - WordCount
        // Spark是一个计算框架
        // 开发人员是使用Spark框架的API实现计算功能

        // TODO 1.准备Spark环境
        // setMaster: 设定Spark环境的位置
        // local表示本地模式，如果local后不增加任何的内容，表示单线程操作
        // 如果想要多线程操作，可以增加配置local[2]、local[*]
        val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount").setMaster("local")

        // TODO 2.建立和Spark的连接
        // 相当于jdbc: connection
        val sc = new SparkContext(sparkConf)

        // TODO 3.实现业务逻辑
        // TODO 3.1 读取指定目录下的数据文件（多个）
        // 参数 path 可以指向单一的文件，也可以指向文件目录
        // RDD: 更适合并行计算的数据模型
        val fileRDD: RDD[String] = sc.textFile("input")

        // TODO 3.2 将读取的内容进行扁平化操作，切分单词
        val wordRDD: RDD[String] = fileRDD.flatMap((_: String).split(" "))

        // TODO 3.3 将分词后的数据进行分组（按单词分组）
        val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy((word: String) => word)

        // TODO 3.4 将分组后的数据进行聚合：(word, Iterable) => (word, count)
        val mapRDD: RDD[(String, Int)] = groupRDD.map {
            case (word, iter) => {
                (word, iter.size)
            }
        }

        // TODO 3.5 将聚合的结果采集后打印到控制台
        val wordCountArray: Array[(String, Int)] = mapRDD.collect()
        println(wordCountArray.mkString(","))

        // TODO 4.释放连接
        sc.stop()

    }

}
