package com.learning.xiaoniuxuetang.sparkstreamingredis


/**
import cn.edu360.xiaoniu.sparkstreaming.ZkSerializers
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Author: tanggaomeng
  * Date: 2020/9/24 10:58
  * Describe:
  */
object OrderCount {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("OrderCount").setMaster("local[*]")
    // 创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Duration(5000))

    // 整理ip规则数据
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = IPUtils.broadcastIpRules(ssc, "hdfs://managerhd.bigdata:8020/learning/ip/")

    // 指定kafka组名
    val group = "g1"
    // 指定消费的kafka的topic
    val topic = "test1"
    // 指定kafka的broker地址（SparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高）
    val brokerList = "managerhd.bigdata:6667"
    // 指定zk的地址，后期更新消费的偏移量时使用（以后可以使用Redis、MySQL来记录偏移量）
    val zkQuorum = "managerhd.bigdata:2181,masterhd.bigdata:2181,workerhd.bigdata:2181"

    // 创建Stream时使用topic名字集合，SparkStreaming可同时消费多个topic
    val topics: Set[String] = Set(topic)
    // 创建一个ZKGroupTopicDirs对象，其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    // 获取zk中的路径，"/g1/offsets/test/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    // 准备kafka的参数
    val kafkaParams: Map[String, String] = Map(
      //"key.deserializer" -> classOf[StringDeserializer],
      //"value.deserializer" -> classOf[StringDeserializer],
      //"deserializer.encoding" -> "GB2312", //配置读取Kafka中数据的编码
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    // zk的host和ip，创建一个client，用于更新偏移量的是zk的客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)
    // 序列化zk
    zkClient.setZkSerializer(new ZkSerializers())

    // 查询该路径下是否有子节点，默认有子节点为我们自己保存不同partition时生成
    val children: Int = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String, String)] = null

    // 如果zk中有保存offset，我们会利用这个offset作为kafkaStream的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    // 如果保存过offset
    // 注意：偏移量的查询是在Driver端完成的
    if (children > 0) {
      for (i <- 0 until children) {
        // /g1/offset/test1/0/10001

        // /g1/offset/test1/0
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/${ i }")
        // test1/0
        val tp = TopicAndPartition(topic, i)
        // 将不同partition对用的offset增加到fromoffsets中
        // /test1/0 --> 10001
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      // key：kafka的key
      // value："hello tom hello jerry"
      // 这个会将kafka的消息进行transform，最终kafka的数据都会变成（kafka的key，message）这样的tuple
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      // 通过kafkaUtils创建直连的DStream（fromOffset参数的作用是，按照前面计算好了的偏移量继续消费数据）
      //[String, String, StringDecoder, StringDecoder,   (String, String)]
      //  key    value    key的解码方式   value的解码方式    返回结果
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    } else {
      // 如果zk中未保存数据，根据kafkaParam的配置使用最新（largest）或者使用最旧的（smallest）offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    // 偏移量的范围
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    // 直连方式只有在KafkaDStream的RDD（KafkaRDD）中才能获取偏移量，
    // 那么就不能到调用DStream的Transformation
    // 所以只能在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
    // 依次迭代KafkaDStream中的KafkaRDD
    // 如果使用直连方式累加数据，那么就要在外部的数据库中进行累加（用KeyVlaue的内存数据库（NoSQL），Redis）
    // kafkaStream.foreachRDD里面的业务逻辑是在Driver端执行
    kafkaStream.foreachRDD{ kafkaRDD: RDD[(String, String)] =>
      // 判断当前的kafkaStream中的RDD是否有数据
      if (!kafkaRDD.isEmpty()){
        // 只有kafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        val lines: RDD[String] = kafkaRDD.map((_: (String, String))._2)
        // 整理数据
        val fields: RDD[Array[String]] = lines.map((_: String).split("[ ]"))

        // 计算成交总金额
        CalculateUtil.calculateIncome(fields)

        // 计算商品分类金额
        CalculateUtil.calculateItem(fields)

        // 计算区域成交金额
        CalculateUtil.calculateZone(fields, broadcastRef)

        // 偏移量更新
        for (o <- offsetRanges) {
          // /g1/offset/test1/0
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          // 将该partition保存到zk
          // /g1/offset/test1/0/20000
          ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
        }

      }

    }

    // 启动
    ssc.start()
    // 优雅的退出
    ssc.awaitTermination()

  }

}
*/