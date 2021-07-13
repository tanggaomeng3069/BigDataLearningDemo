package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark40_RDD_Operator22 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）
        //  RDD默认只能操作单值类型数据
        //  然是通过隐式转换，可以通过扩展操作双值操作

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark40_RDD_Operator22")
        val sc = new SparkContext(sparkConf)

        // TODO 将分区内相同的key取最大值，分区间相同的key求和

        //  0 => [(a,2),(c,3)]
        //                      => [(a,2),(b,4),(c,9)]
        //  1 => [(b,4),(c,6)]

        // TODO 分区内和分区间的计算规则不一样
        //  reduceByKey：分区内和分区间的计算规则一样，都是相同的key求和
        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("c", 3),
            ("b", 4), ("c", 5), ("c", 6)
        ), 2)

        // TODO aggregateByKey: 根据key进行数据聚合
        //  Scala语法：函数柯里化
        //  方法中有两个参数列表需要传递参数
        //  第一个参数列表传递参数为zeroValue：计算的初始值
        //    用于在分区内进行计算时，当作初始值进行计算
        //  第二个参数列表传递参数为：
        //      seqOp：分区内的计算规则，相同的key的value的计算
        //      combOp：分区间的计算规则，相同的key的value的计算
//        val resultRDD: RDD[(String, Int)] = dataRDD.aggregateByKey(0)(
//            (x, y) => x + y,
//            (x, y) => x + y
//        )

//        val resultRDD: RDD[(String, Int)] = dataRDD.aggregateByKey(0)(_+_, _+_)

        // TODO 如果分区内计算规则和分区间计算规则相同，那么可以将aggregateByKey简化为foldByKey
        val resultRDD: RDD[(String, Int)] = dataRDD.foldByKey(0)(_+_)


        println(resultRDD.collect().mkString(","))


        sc.stop()

    }

}
