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
object Spark02_Req1_HotCategoryTop10Analysis1 {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // Q: dataRDD每次统计信息的时候都使用，重复使用
        // Q: cogroup性能可能较低

        // TODO 1.读取原始日志数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
        // 缓存
        dataRDD.cache()

        // TODO 2.统计品类的点击数量：（品类ID，点击数量）
        val clickActionRDD: RDD[String] = dataRDD.filter(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                datas(6) != "-1"
            }
        )

        val clickActionCountRDD: RDD[(String, Int)] = clickActionRDD.map(
            (action: String) => {
                val datas: mutable.ArrayOps[String] = action.split("_")
                (datas(6), 1)
            }
        ).reduceByKey((_: Int) + (_: Int))

//        clickActionCountRDD.foreach(println)
//        println("*"*100)

        // TODO 3.统计商品的下单数量：（品类ID，下单数量）
        val orderActionRDD: RDD[String] = dataRDD.filter(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                datas(8) != "null"
            }
        )
        val orderActionCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
            (action: String) => {
                val datas: mutable.ArrayOps[String] = action.split("_")
                val cid: StringOps = datas(8)
                val cids: mutable.ArrayOps[String] = cid.split(",")
                cids.map((id: String) => (id, 1))
            }
        ).reduceByKey((_: Int) + (_: Int))

//        orderActionCountRDD.foreach(println)
//        println("*"*100)

        // TODO 4.统计商品的支付数量：（品类ID， 支付数量）
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

//        payActionCountRDD.foreach(println)
//        println("*"*100)

        // （品类ID，点击数量） => （品类ID，（点击数量， 0， 0））
        // （品类ID，下单数量） => （品类ID，（0， 下单数量， 0））
        //                   => （品类ID，（点击数量， 下单数量， 0））
        // （品类ID，支付数量） => （品类ID，（0， 0， 支付数量））
        //                   => （品类ID，（点击数量，下单数量，支付数量））
        // （品类ID，（点击数量，下单数量，支付数量））

        // TODO 5.将品类进行排序，并且取前10名
        //  点击数量排序，下单数量排序，支付数量排序
        //  元组排序，先比较第一个，再比较第二个，再比较第三个，以此类推

        val rdd1: RDD[(String, (Int, Int, Int))] = clickActionCountRDD.map {
            case (cid, cnt) => {
                (cid, (cnt, 0, 0))
            }
        }

        val rdd2: RDD[(String, (Int, Int, Int))] = orderActionCountRDD.map {
            case (cid, cnt) => {
                (cid, (0, cnt, 0))
            }
        }

        val rdd3: RDD[(String, (Int, Int, Int))] = payActionCountRDD.map {
            case (cid, cnt) => {
                (cid, (0, 0, cnt))
            }
        }

        // 将三个数据源合并在一起，统一进行计算
        val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

        val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
            (t1: (Int, Int, Int), t2: (Int, Int, Int)) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )
        val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy((_: (String, (Int, Int, Int)))._2, ascending = false).take(10)

        resultRDD.foreach(println)


        sc.stop()
    }

}
