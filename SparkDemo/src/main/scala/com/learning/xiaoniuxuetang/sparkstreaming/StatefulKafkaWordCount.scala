package com.learning.xiaoniuxuetang.sparkstreaming


/**
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2020/9/22 20:09
  * Describe:
  */
object StatefulKafkaWordCount {

  /**
    * 第一个参数：聚合的key，就是单词
    * 第二个参数：当前批次产生批次该单词在每一个分区出现的次数
    * 第三个参数：初始值或累加的中间结果
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.map {
      case (x, y, z) => (x, y.sum, z.getOrElse(0))
    }
  }


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("StatefulKafkaWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // 如果要使用可更新历史数据（累加），那么就要把中间结果保存起来
    ssc.checkpoint("./ck")

    val zkQuorum = "managerhd.bigdata:2181,masterhd.bigdata:2181,workerhd.bigdata:2181"
    val groupId = "g1"
    val topic: Map[String, Int] = Map[String, Int]("test1" -> 1)

    // 创建DStream，需要kafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    // 对数据进行处理
    // kafka的ReceiverInputDStream[(String, String)]里面装的是一个元组，
    // key是写入的key，value是实际写入的内容
    val lines: DStream[String] = data.map((_: (String, String))._2)
    // 切分压平
    val words: DStream[String] = lines.flatMap((_: String).split(" "))
    // 单词和1组合
    val wordAndOne: DStream[(String, Int)] = words.map(((_: String), 1))
    // 聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey((_: Int) + (_: Int))

    //wordAndOne.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    //val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    reduced.print()

    // 启动
    ssc.start()
    // 优雅的退出
    ssc.awaitTermination()


  }

}
*/