package com.learning.xiaoniuxuetang.sparkstreaming

/*
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
*/
/**
  * Author: tanggaomeng
  * Date: 2020/9/22 20:29
  * Describe:
  *  使用pom.xml旧版本kafka：spark-streaming-kafka-0-8_2.11
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {
/**
    // 创建sparkConf
    val conf: SparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[*]")
    // 创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Duration(5000))
    // 指定kafka的broker地址，sparkstream的Task直连到kafka的分区上，用更底层的API消费，效率更高
    val brokerList = "managerhd.bigdata:6667"
    // 执行zk的地址，后期更新消费的偏移量时使用，以后可以使用Redis，MySQL来记录偏移量
    val zkQuorum = "managerhd.bigdata:2181,masterhd.bigdata:2181,workerhd.bigdata:2181"
    // 执行kafka组名
    val group = "g1"
    // 指定消费者名称
    val topic = "test1"
    // 创建stream时使用的topic名字集合，SparkStreaming可同时消费多个topic
    val topics: Set[String] = Set(topic)

    // 创建一个ZKGroupTopicDirs对象，其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    // 获取zk中的路径 "/g1/offsets/test1/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    // 准备kafka的参数
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    // zookeeper的host和ip，创建一个client，用于更新偏移量的是zk的客户端
    // 可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)
    // 序列化
    zkClient.setZkSerializer(new ZkSerializers())

    // 查询该路径下是否有子节点，默认有子节点为我们自己保存不同partition时生成
    // /g1/offsets/test1/0/10001"
    // /g1/offsets/test1/1/30001"
    // /g1/offsets/test1/2/10001"
    //zkTopicPath  -> /g1/offsets/test1/
    val children: Int = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String, String)] = null

    // 如果zk中有保存offset，我们会利用这个offset作为kafkaStream的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    // 如果保存过offset
    if (children > 0) {
      for (i <- 0 until children) {
        // /g1/offsets/test1/0/10001

        // /g1/offsets/test1/0
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/${ i }")
        // test1/0
        val tp = TopicAndPartition(topic, i)
        // 将不同的partition对应的offset增加到fromOffset中
        // test1/0 -> 10001
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      // key：kafka的key
      // values："hello tom hello jerry"
      // 这个会将kafka的消息进行transform，最终kafka的数据都会变成（kafka的key，message）这样的tuple
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      // 通过kafkaUtils创建直连的DStream（fromOffsets参数的作用是：按照前面计算好了的偏移量继续消费数据）
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      // 如果未保存，根据kafkaParam的配置使用最新的（largest）或者最旧的（smallest）offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    // 偏移量的范围
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    // 从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
    // 该transform方法计算获取当前批次RDD，然后将RDD的偏移量取出来，然后在将RDD返回到DStream
    val transform: DStream[(String, String)] = kafkaStream.transform { rdd: RDD[(String, String)] =>
      // 得到该RDD对应kafka的消息offset
      // 该RDD是一个kafkaRDD，可以获得偏移量的范围
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val messages: DStream[String] = transform.map((_: (String, String))._2)

    // 依次迭代DStream种的RDD
    messages.foreachRDD{ rdd: RDD[String] =>
      // 对RDD进行操作，触发Action
      rdd.foreachPartition((partition: Iterator[String]) =>
        partition.foreach((x: String) => {
          println(x)
        })
      )

      for (o <- offsetRanges) {
        //  /g1/offsets/test1/0
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        // 将该partition的offset保存到zk
        //  /g1/offsets/test1/0/20000
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    }

    // 启动
    ssc.start()
    // 优雅的退出
    ssc.awaitTermination()

  */
  }
}
