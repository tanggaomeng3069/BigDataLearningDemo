package com.learning.xiaoniuxuetang.sparkstreaming


/**
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/9/18 8:55
  * Describe:
  */
object kafkaWordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("kafkaWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val zkQuorum = "managerhd.bigdata:2181,masterhd.bigdata:2181,workerhd.bigdata:2181"
    val groupId = "g1"
    val topic: Map[String, Int] = Map[String, Int]("test1" -> 1)

    // 创建DStream，需要kafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    // 对数据进行处理
    // kafka的ReceiverInputDStream[(String, String)]里面装的是一个元组
    // key是写入的key，value是实际写入的内容
    val lines: DStream[String] = data.map((_: (String, String))._2)
    // 对DStream进行操作
    // 切分压平
    val words: DStream[String] = lines.flatMap((_: String).split(" "))
    // 单词和1组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map(((_: String), 1))
    // 聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey((_: Int) + (_: Int))
    // 打印结果
    reduced.print()

    // 启动sparkstreaming
    ssc.start()
    // 优雅的退出
    ssc.awaitTermination()

  }

}
*/