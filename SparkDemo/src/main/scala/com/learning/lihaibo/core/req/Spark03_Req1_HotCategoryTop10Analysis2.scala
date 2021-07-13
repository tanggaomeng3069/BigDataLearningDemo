package com.learning.lihaibo.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
  * Author: tanggaomeng
  * Date: 2021/1/13 9:31
  * Describe:
  */
object Spark03_Req1_HotCategoryTop10Analysis2 {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // Q: reduceByKey存在大量shuffle操作
        // reduceByKey聚合算子，spark会提供优化，缓存

        // TODO 1.读取原始日志数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

        // TODO 2.将数据转换结构
        //  点击的场合：（品类ID，（1，0，0））
        //  下单的场合：（品类ID，（0，1，0））
        //  支付的场合：（品类ID，（0，0，1））
        val flatRDD: RDD[(String, (Int, Int, Int))] = dataRDD.flatMap(
            (action: String) => {
                val datas: mutable.ArrayOps[String] = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    List((datas(8), (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付的场合
                    List((datas(10), (0, 0, 1)))
                } else {
                    Nil
                }
            }
        )

        // TODO 3.将相同的品类ID的数据进行分组聚合
        val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
            (t1: (Int, Int, Int), t2: (Int, Int, Int)) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )

        // TODO 4.将统计结果根据数据量降序处理，取前10条
        val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy((_: (String, (Int, Int, Int)))._2, ascending = false).take(10)

        // TODO 5.将结果采集到控制台打印出来
        resultRDD.foreach(println)

        sc.stop()
    }

}
