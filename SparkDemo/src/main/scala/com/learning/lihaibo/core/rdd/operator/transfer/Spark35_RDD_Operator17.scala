package com.learning.lihaibo.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/11/14 16:21
  * Describe:
  */
object Spark35_RDD_Operator17 {
    def main(args: Array[String]): Unit = {

        // TODO Spark - RDD 算子（方法）
        //  RDD默认只能操作单值类型数据
        //  然是通过隐式转换，可以通过扩展操作双值操作

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark35_RDD_Operator17")
        val sc = new SparkContext(sparkConf)

        // TODO 自定义分区器 -- 自己决定数据放置在哪个分区做处理
        //  cba, wnba, nba
        val dataRDD: RDD[(String, String)] = sc.makeRDD(List(
            ("cba", "消息1"), ("cba", "消息2"), ("cba", "消息3"),
            ("nba", "消息4"), ("wnba", "消息5"), ("nba", "消息6")
        ), 1)

        val rdd1: RDD[(String, String)] = dataRDD.partitionBy(new MyPartitioner(3))
        val rdd2: RDD[(Int, (String, String))] = rdd1.mapPartitionsWithIndex(
            (index, datas) => {
                datas.map(
                    data => (index, data)
                )
            }
        )
        rdd2.collect().foreach(println)

        sc.stop()

    }

    // TODO 自定义分区器
    //  1.和Partitioner类发生关联，继承Partitioner
    //  2.重写方法 (快捷键 Control + i)
    class MyPartitioner(num: Int) extends Partitioner{

        // 获取分区的数量
        override def numPartitions: Int = {
            num
        }

        // 根据数据的key来决定数据在哪儿分区中进行处理
        // 方法的返回值表示分区的编号
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case _ => 1
            }
        }
    }

}
