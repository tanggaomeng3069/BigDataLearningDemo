package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark34_RDD_Operator16 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）
        //  RDD默认只能操作单值类型数据
        //  然是通过隐式转换，可以通过扩展操作双值操作

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark34_RDD_Operator16")
        val sc = new SparkContext(sparkConf)

        // TODO K-V类型的数据操作
        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("b", 2), ("c", 3)
        ), 1)

        // TODO Spark中很多的方法是基于Key进行操作，所以数据格式应该为键值对（对偶元组）
        //  如果数据类型为K-V类型，那么Spark会给RDD自动补充很多新的功能（扩展）
        //  隐式转换（A => B）
        //  partitionBy方法来自于PairRDDFunctions类
        //  RDD的伴生对象中提供了隐式函数可以将RDD[K,V]转化为PairRDDFunctions类

        // TODO partitionBy根据指定的规则对数据进行分区
        //  groupBy
        //  filter => coalesce 缩减分区
        //  repartition => shuffle 增加分区
        //
        //  partitionBy参数为分区器对象
        //  分区器对象：HashPartitioner & RangePartitioner
        //
        //  HashPartitioner分区规则是将当前数据的key进行取余操作。
        //  HashPartitioner是Spark默认的分区器
        val rdd1: RDD[(String, Int)] = dataRDD.partitionBy(new HashPartitioner(2))
        //rdd1.saveAsTextFile("output")

        // sortBy使用了 RangePartitioner
        //rdd1.sortBy()


        sc.stop()

    }

}
