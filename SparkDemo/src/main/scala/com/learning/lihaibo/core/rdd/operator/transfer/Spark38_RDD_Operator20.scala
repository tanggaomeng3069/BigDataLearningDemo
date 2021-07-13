package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark38_RDD_Operator20 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）
        //  RDD默认只能操作单值类型数据
        //  然是通过隐式转换，可以通过扩展操作双值操作

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark38_RDD_Operator20")
        val sc = new SparkContext(sparkConf)

        // TODO groupByKey: 根据数据的key进行分组
        //  groupBy: 根据指定的规则对数据进行分组
        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("hello", 1), ("scala", 1), ("hello", 1)
        ))

        // TODO 调用groupByKey之后，返回的数据是一个元组
        //  元组的第一个元素表示是分组的key
        //  元组的第二个元素表示是分组后，相同的key的value的集合
        val rdd1: RDD[(String, Iterable[Int])] = dataRDD.groupByKey()

        val rdd2: RDD[(String, Int)] = rdd1.map {
            case (word, iter) => {
                (word, iter.sum)
            }
        }

        println(rdd2.collect().mkString(","))


        sc.stop()

    }

}
