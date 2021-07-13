package com.learning.lihaibo.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author: tanggaomeng
  * Date: 2021/1/12 15:45
  * Describe:
  */
object Spark65_BC1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO Spark广播变量
        //  join会有笛卡尔乘积效果，数据量急剧增多，如果有shuffle操作，那么性能会非常低

        // TODO 为了解决join出现性能问题，可以将数据独立出来，防止shuffle操作。
        //  这样的话，会导致数据每一个task会复制一份，那么Executor内存中会有大量冗余数据，性能大幅度下降，
        //  所以可以采用广播变量，将数据保存到Executor的内存中。

        val dataRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val list = List(("a", 4), ("b", 5), ("c", 6))

        val RDD2: RDD[(String, (Int, Int))] = dataRDD1.map {
            case (word, count1) => {
                var count2 = 0
                for (kv <- list) {
                    val w: String = kv._1
                    val v: Int = kv._2
                    if (w == word) {
                        count2 = v
                    }
                }
                (word, (count1, count2))
            }
        }
        println(RDD2.collect().mkString(","))

        sc.stop()

    }
}
