package com.learning.lihaibo.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/12/12 13:37
  * Describe:
  */
object Spark47_Operator_Action1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
        val sc = new SparkContext(sparkConf)

        // TODO Spark 算子 - 行动
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO reduce
        //  简化，规约
        val result: Int = dataRDD.reduce(_+_)
        println(result)

        // TODO collect
        //  采集数据
        //  collect方法会将所有分区计算的结果拉取到当前节点的内存中，可能会造成内存溢出
        val ints: Array[Int] = dataRDD.collect()
        ints.foreach(println)

        // TODO count
        //  统计数据量
        val cont: Long = dataRDD.count()
        println(cont)

        // TODO first
        //  第一个元素
        val fir: Int = dataRDD.first()
        println(fir)

        // TODO take
        //  取前几元素
        val take3: Array[Int] = dataRDD.take(3)
        println(take3.mkString(","))


        sc.stop()

    }

}
