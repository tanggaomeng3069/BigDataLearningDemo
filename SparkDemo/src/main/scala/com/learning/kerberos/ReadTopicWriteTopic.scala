package com.learning.kerberos

import java.util.Properties

import com.learning.utils.{KafkaSink, KerberosConfigLoader}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext

/**
 * SparkStreaming从Kafka中读取数据，写入到Kafka
 */
object ReadTopicWriteTopic {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        val javaSecurityKrb5Conf = KerberosConfigLoader.getString("java.security.krb5.conf")
        val javaSecurityAuthLoginConfig = KerberosConfigLoader.getString("java.security.auth.login.config")

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
        System.setProperty("java.security.krb5.conf", javaSecurityKrb5Conf)
        System.setProperty("java.security.auth.login.config", javaSecurityAuthLoginConfig)

        val spark: SparkSession = SparkSession
            .builder()
            .appName("ReadKafkaWriteKafka")
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .master("local[*]")
            .getOrCreate()

        val sc: SparkContext = spark.sparkContext
        val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

        // 加载配置文件
        val brokers: String = KerberosConfigLoader.getString("kafka.broker.list")
        val topics: String = KerberosConfigLoader.getString("kafka.topic.read")
        val topicWrite: String = KerberosConfigLoader.getString("kafka.topic.write")
        if (StringUtils.isEmpty(brokers) || StringUtils.isEmpty(topics)) {
            println("未配置 Kafka 信息...")
            System.exit(0)
        }
        // 处理topic字符串
        val topicsSet: Set[String] = topics.split(",").toSet

        val kafkaParams = Map(
            "bootstrap.servers" -> brokers, // 用于初始化链接到集群的地址
            "sasl.kerberos.service.name" -> "kafka",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            // 用于标识这个消费者属于哪个消费团体
            "group.id" -> KerberosConfigLoader.getString("kafka.group.id"),
            // 设置Kafka认证协议
            "security.protocol" -> "SASL_PLAINTEXT",
            // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，
            // 可使用这个配置，latest自动重置偏移量为最新的偏移量
            "auto.offset.reset" -> "latest",
            // 如果是true，则这个消费者的偏移量会在后台自动提交
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )

        // kafka sink config
        val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
            val kafkaProducerConfig: Properties = {
                val p = new Properties()
                p.setProperty("bootstrap.servers", brokers)
                p.setProperty("key.serializer", classOf[StringSerializer].getName)
                p.setProperty("value.serializer", classOf[StringSerializer].getName)
                //在kerberos环境下，以下配置需要增加
                p.setProperty("security.protocol", "SASL_PLAINTEXT")
                p.setProperty("sasl.mechanism", "GSSAPI")
                p.setProperty("sasl.kerberos.service.name", "kafka")
                p
            }
            ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
        }

        // 从kafka中读取数据
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
                        // 写入到Kafka
                        val sinkData: String = line.value()
                        kafkaProducer.value.send(topicWrite, sinkData)
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
