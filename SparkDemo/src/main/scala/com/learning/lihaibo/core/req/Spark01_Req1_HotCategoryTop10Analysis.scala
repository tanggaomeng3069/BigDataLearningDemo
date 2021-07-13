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
object Spark01_Req1_HotCategoryTop10Analysis {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // TODO 1.读取原始日志数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

        // TODO 2.统计品类的点击数量：（商品ID，点击数量）
        val clickActionRDD: RDD[String] = dataRDD.filter(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                datas(6) != "-1"
            }
        )

        val clickActionCountRDD: RDD[(String, Int)] = clickActionRDD.map(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                (datas(6), 1)
            }
        ).reduceByKey((_: Int) + (_: Int))

        // TODO 3.统计商品的下单数量：（商品ID，下单数量）
        val orderActionRDD: RDD[String] = dataRDD.filter(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                datas(8) != "null"
            }
        )

        val orderActionCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
            (action: String) => {
                val datas: mutable.ArrayOps[String] = action.split("_")
                val cid: String = datas(8)
                val cids: mutable.ArrayOps[String] = cid.split(",")
                cids.map((id: String) => (id, 1))
            }
        ).reduceByKey((_: Int) + (_: Int))

        // TODO 4.统计商品的支付数量：（商品ID，支付数量）
        val payActionRDD: RDD[String] = dataRDD.filter(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                datas(10) != "null"
            }
        )

        val payActionCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
            (action: String) => {
                val datas: mutable.ArrayOps[String] = action.split("_")
                val cid: StringOps = datas(10)
                val cids: mutable.ArrayOps[String] = cid.split(",")
                cids.map((id: String) => (id, 1))
            }
        ).reduceByKey((_: Int) + (_: Int))

        // TODO 5.将商品进行排序，并且取前10名
        //  点击数量排序，下单数量排序，支付数量排序
        //  元组排序：先比较第一个，再比较第二个，再比较第三个，以此类推
        //  （商品ID，（点击数量， 下单数量，支付数量））

        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
            clickActionCountRDD.cogroup(orderActionCountRDD, payActionCountRDD)
        val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
            case (clickIter, orderIter, payIter) => {

                var clickCnt = 0
                val iter1: Iterator[Int] = clickIter.iterator
                if (iter1.hasNext) {
                    clickCnt = iter1.next()
                }

                var orderCnt = 0
                val iter2: Iterator[Int] = orderIter.iterator
                if (iter2.hasNext) {
                    orderCnt = iter2.next()
                }

                var parCnt = 0
                val iter3: Iterator[Int] = payIter.iterator
                if (iter3.hasNext) {
                    parCnt = iter3.next()
                }

                (clickCnt, orderCnt, parCnt)

            }
        }

        val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy((_: (String, (Int, Int, Int)))._2, ascending = false).take(10)

        // TODO 6.将结果采集到控制台打印出来
        resultRDD.foreach(println)


        sc.stop()
    }

}
