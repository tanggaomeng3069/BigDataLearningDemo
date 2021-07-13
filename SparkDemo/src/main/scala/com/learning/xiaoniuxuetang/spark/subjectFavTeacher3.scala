package com.learning.xiaoniuxuetang.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable



/**
  * Author: tanggaomeng
  * Date: 2020/8/24 16:54
  * Describe:
  */
object subjectFavTeacher3 {

  def main(args: Array[String]): Unit = {

    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName("subjectFavTeacher3").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines: RDD[String] = sc.textFile("hdfs://managerhd.bigdata:8020/learning/spark/teacher.log")

    // 处理数据
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map((lines: String) => {
      val index: Int = lines.lastIndexOf("/")
      val teacher: String = lines.substring(index + 1)
      val httpHost: String = lines.substring(0, index)
      val subject: String = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    // 聚合，将学科和老师联合当作key
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey((_: Int) + (_: Int))

    // 计算有多少学科
    val subjects: Array[String] = reduced.map((_: ((String, String), Int))._1._1).distinct().collect()

    // 自定义一个分区器，并且按照指定的分区器进行分区
    val sbPartitioner = new SubjectParitioner(subjects)

    // partitionBy按照指定的分区规则进行区分
    // 调用partitionBy时RDD的key是(String, String)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPartitioner)

    // 如果一次拿出一个分区（可以操作一个分区中的数据）
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions((it: Iterator[((String, String), Int)]) => {
      // 将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy((_: ((String, String), Int))._2).reverse.take(3).iterator
    })

    // 采集结果
    val result: Array[((String, String), Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()

  }
}


// 自定义分区器
class SubjectParitioner(sbs: Array[String]) extends Partitioner{

  // 相当于主构造器（new的时候执行一次）
  // 用于存放规则的一个map
  private val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <- sbs){
//    rules(sb) = i
    rules.put(sb, i)
    i += 1
  }

  // 返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = sbs.length

  // 根据传入的key，计算分区标号
  // key是一个元组(String, String)
  override def getPartition(key: Any): Int = {

    // 获取学科名称
    val subject: String = key.asInstanceOf[(String, String)]._1

    // 根据规则计算分区编号
    rules(subject)
  }
}
