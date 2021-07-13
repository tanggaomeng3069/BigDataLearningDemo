package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark27_RDD_Operator9 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark27_RDD_Operator9")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))
        // TODO Spark - RDD 算子（方法）
        //  Sample：用于从数据集中抽取数据
        //  第一个参数表示数据抽取后是否有放回，可以重复抽取
        //    true：抽取后放回
        //    false：抽取后不放回
        //  第二个参数表示数据抽取的几率（不放回的场合）。重复抽取的次数（放回的场合）
        //    这里的几率不是数据被抽取的数据总量的比率，是数据集合中每个数据被抽取的几率。
        //  第三个参数表示随机数种子，可以确定数据的抽取，随机数确定之后，抽取的集合确定不变
        //    随机数不随机，所谓的随机数依靠随机算法实现

        val dataRDD: RDD[Int] = rdd.sample(false,0.5)
        println(dataRDD.collect().mkString(","))
        // 1,3（集合不确定，每次执行不同）

        val dataRDD1: RDD[Int] = rdd.sample(true, 2)
        println(dataRDD1.collect().mkString(","))
        // 1,1,1,1,1,1,1,2,2,2,3,3,3,4,6（集合不确定，每次执行不同）


        val dataRDD2: RDD[Int] = rdd.sample(false,0.5, 1)
        println(dataRDD2.collect().mkString(","))
        // 3,5（集合确定，每次执行相同）

        // TODO 应用场景：
        //  在实际的开发中，往往会出现数据倾斜的情况，那么可以从数据倾斜的分区中抽取数据，
        //  查看数据的规则，分析后，可以进行改善处理，让数据更加均匀。



        sc.stop()

    }

}
