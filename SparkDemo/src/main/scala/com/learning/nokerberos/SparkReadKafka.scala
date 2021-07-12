package com.learning.nokerberos

import com.learning.utils.noKerberosConfigLoader
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadKafka {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf()
            .setAppName("SparkOnKafka")
            .setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        // 创建SparkStreaming并设置间隔时间10s
        val ssc = new StreamingContext(sc, Seconds(10))

        val brokers: String = noKerberosConfigLoader.getString("kafka.broker.list")
        val topics: String = noKerberosConfigLoader.getString("kafka.topic")
        val topicsSet: Set[String] = topics.split(",").toSet

        val kafkaParams = Map(
            "bootstrap.servers" -> brokers, // 用于初始化链接到集群的地址
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            // 用于标识这个消费者属于哪个消费团体
            "group.id" -> noKerberosConfigLoader.getString("kafka.group.id"),
            // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，
            // 可使用这个配置，latest自动重置偏移量为最新的偏移量
            "auto.offset.reset" -> "latest",
            // 如果是true，则这个消费者的偏移量会在后台自动提交
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
        )

        kafkaDStream.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                println("kafkaRDD is not Empty!^^!")
                rdd.foreachPartition(partitionRecords => {
                    partitionRecords.foreach(line => {
                        println(line.value())
                        // 补充代码逻辑
                    })
                })
            } else {
                println("kafkaRDD is Empty!~~!")
            }
        })

        // 启动
        ssc.start()
        // 关闭
        ssc.awaitTermination()




    }

}
