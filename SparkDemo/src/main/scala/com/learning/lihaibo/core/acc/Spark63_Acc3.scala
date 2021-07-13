package com.learning.lihaibo.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Author: tanggaomeng
  * Date: 2021/1/12 15:45
  * Describe:
  */
object Spark63_Acc3 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        // TODO 累加器：WordCount
        val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello", "spark", "scala"))

        // TODO 1. 创建累加器
        val acc: MyWordCountAccumulator = new MyWordCountAccumulator

        // TODO 2. 注册累加器
        sc.register(acc)

        // TODO 3. 使用累加器
        dataRDD.flatMap((_: String).split(" ")).foreach{
            word: String => {
                acc.add(word)
            }
        }

        // TODO 4. 获取累加器的值
        println(acc.value)

        sc.stop()

    }

    // TODO 自定义累加器
    //  1. 继承 AccumulatorV2

    class MyWordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

        // TODO 声明存储WordCount的空集合
        var wordCountMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

        // TODO 累加器是否初始化
        override def isZero: Boolean = {
            wordCountMap.isEmpty
        }

        // TODO 复制累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
            new MyWordCountAccumulator
        }

        // TODO 重置累加器
        override def reset(): Unit = {
            wordCountMap.clear()
        }

        // TODO 向累加器中增加值
        override def add(word: String): Unit = {
            // 以下两种写法作用相同
            wordCountMap(word) = wordCountMap.getOrElse(word, 0) + 1
            //wordCountMap.update(word, wordCountMap.getOrElse(word, 0) + 1)
        }

        // TODO 合并当前累加器和其他累加器
        //  合并累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
            val map1: mutable.Map[String, Int] = wordCountMap
            val map2: mutable.Map[String, Int] = other.value

            wordCountMap = map1.foldLeft(map2)(
                (map: mutable.Map[String, Int], kv: (String, Int)) => {
                    map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
                    map
                }
            )
        }

        // TODO 返回累加器的值
        override def value: mutable.Map[String, Int] = {
            wordCountMap
        }
    }
}
