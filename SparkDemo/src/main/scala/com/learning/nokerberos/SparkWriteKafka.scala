package com.learning.nokerberos

import java.util.Properties

import com.learning.utils.{KafkaSink, noKerberosConfigLoader}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteKafka {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf()
            .setAppName("SparkWriteKafka")
            .setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        // 示例数据
        val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

        val brokers: String = noKerberosConfigLoader.getString("kafka.broker.list")
        val topics: String = noKerberosConfigLoader.getString("kafka.topic")

        // kafka sink config
        val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
            val kafkaProducerConfig: Properties = {
                val p = new Properties()
                p.setProperty("bootstrap.servers", brokers)
                p.setProperty("key.serializer", classOf[StringSerializer].getName)
                p.setProperty("value.serializer", classOf[StringSerializer].getName)
                p
            }
            sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
        }

        // 写入Kafka
        dataRDD.foreachPartition(partition => {
            partition.foreach(line => {
                val sinkData: String = line.toString
                kafkaProducer.value.send(topics, sinkData)
            })
        })


        sc.stop()
    }

}
