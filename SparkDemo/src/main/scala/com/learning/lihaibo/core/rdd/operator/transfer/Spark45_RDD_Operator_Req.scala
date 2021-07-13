package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark45_RDD_Operator_Req {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark45_RDD_Operator_Req")
        val sc = new SparkContext(sparkConf)

        // TODO 统计出每一个省份每一个广告被点击数量排行榜的Top3

        // TODO 1.获取原始数据
        val dataRDD: RDD[String] = sc.textFile("input/agent.log")

        // TODO 2.将原始数据进行转换， 方便统计（（省份 - 广告），1）
        val mapRDD: RDD[(String, Int)] = dataRDD.map((line: String) => {
            val datas: Array[String] = line.split(" ")
            (datas(1) + "-" + datas(4), 1)
        })
        // TODO 3.将相同的key的数据进行分组聚合（（省份 - 广告），sum）
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey((_: Int) + (_: Int))

        // TODO 4.将聚合后的结果进行结构的转换（省份，（广告，sum））
        val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
            case (key, sum) => {
                val keys: Array[String] = key.split("-")
                (keys(0), (keys(1), sum))
            }
        }

        // TODO 5.将相同的省份的数据分在一个组内
        //  （省份，Iterator[（广告1，sum1），（广告2，sum2）]）
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

        // TODO 6.将分组后的数据进行排序（降序），取前3 Top3
        val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
            iter.toList.sortWith(
                (left: (String, Int), right: (String, Int)) => {
                    left._2 > right._2
                }
            ).take(3)
        })

        // TODO 7.将结果打印到控制台
        val result: Array[(String, List[(String, Int)])] = sortRDD.collect()
        result.foreach(println)


        sc.stop()

    }

}
