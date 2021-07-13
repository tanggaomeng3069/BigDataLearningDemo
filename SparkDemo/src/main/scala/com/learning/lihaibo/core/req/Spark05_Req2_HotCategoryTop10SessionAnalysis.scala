package com.learning.lihaibo.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Author: tanggaomeng
  * Date: 2021/1/13 9:31
  * Describe:
  */
object Spark05_Req2_HotCategoryTop10SessionAnalysis {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // TODO 1.读取原始日志数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
        dataRDD.cache()

        val top10Ids: Array[String] = top10Category(dataRDD)

        // TODO 2.过滤原始数据，保留点击和前10品类ID
        val filterActionRDD: RDD[String] = dataRDD.filter(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                if (datas(6) != "-1") {
                    top10Ids.contains(datas(6))
                } else {
                    false
                }
            }
        )

        // TODO 3.根据品类ID和sessionid进行点击量的统计
        val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                ((datas(6), datas(2)), 1)
            }
        ).reduceByKey((_: Int) + (_: Int))

        // TODO 4.将统计的结果进行结构的转换
        //  （（品类ID，sessionid），sum） => （品类ID，（sessionid，sum））
        val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
            case ((cid, sid), sum) => {
                (cid, (sid, sum))
            }
        }

        // TODO 5.相同的品类进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

        // TODO 6.将分组后的数据进行点击量的排序，取前10名
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            (iter: Iterable[(String, Int)]) => {
                iter.toList.sortBy((_: (String, Int))._2)(Ordering.Int.reverse).take(10)
            }
        )

        resultRDD.foreach(println)

        sc.stop()
    }

    def top10Category(actionRDD: RDD[String]): Array[String] = {
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            (action: String) => {
                val datas: mutable.ArrayOps[String] = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids: Array[String] = datas(8).split(",")
                    ids.map((id: String) => (id, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids: Array[String] = datas(10).split(",")
                    ids.map((id: String) => (id, (0, 0, 1)))
                } else {
                    Nil
                }

            }
        )

        val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
            (t1: (Int, Int, Int), t2: (Int, Int, Int)) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )
        analysisRDD.sortBy((_: (String, (Int, Int, Int)))._2,ascending = false).take(10).map((_: (String, (Int, Int, Int)))._1)
    }

}
