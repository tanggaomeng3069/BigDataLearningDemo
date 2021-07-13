package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark41_RDD_Operator23 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）
        //  RDD默认只能操作单值类型数据
        //  然是通过隐式转换，可以通过扩展操作双值操作

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark41_RDD_Operator23")
        val sc = new SparkContext(sparkConf)

        // TODO combineByKey
        //  每个key的平均值：相同的key的总和 / 相同的key的数量

        // 0 => [("a",88),("b",95),("a",91)]
        // 1 => [("b",93),("a",95),("b",98)]
        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 88), ("b", 95), ("a", 91),
            ("b", 93), ("a", 95), ("b", 98)
        ), 2)

        // 88 => (88,1) + 91 => (179,2) + 95 => (274,3)
        // 计算时需要将value的格式发生改变，只需要第一个v发生改变结构即可
        // 如果计算时发现相同key的value不符合计算规则的格式的话，那么选择combineByKey

        // TODO combineByKey方法可以传递三个参数
        //  第一个参数表示的就是将计算的第一个值进行转换结构
        //  第二个参数表示分区内的计算规则
        //  第三个参数表示分区间的计算规则
        val resultRDD: RDD[(String, (Int, Int))] = dataRDD.combineByKey(
            v => (v, 1),
            (t: (Int, Int), v) => {
                (t._1 + v, t._2 + 1)
            },
            (t1: (Int, Int), t2: (Int, Int)) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )

        resultRDD.map {
            case (key, (total, count)) => {
                (key, total / count)
            }
        }.collect().foreach(println)


        sc.stop()

    }

}
