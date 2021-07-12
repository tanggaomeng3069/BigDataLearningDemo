package com.learning.kerberos

import java.util.Properties

import com.learning.utils.{KafkaSink, KerberosConfigLoader}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/9 14:45
 * @Description:
 *              1.Ambari集群开启Kerberos互信模式;
 *              2.Kafka配置 sasl.kerberos.principal.to.local.rules
 *              3.SparkStreaming读取集群1Kafka（Topic：c1_input）中数据
 *              写入集群2Kafka（Topic：c2_output）中
 *              Linux用户在集群1和集群2上都需要创建：zhengzhou
 *              Kerberos用户只在集群1上创建：zhengzhou@INSIGHT.COM
 *              Kerberos用户只在集群2上创建：zhengzhou@INSPUR.COM
 *              4.chown zhengzhou:hadoop /etc/security/keytabs/zhengzhou.keytab
 * @Version 1.0
 */
object ReadKafkaWriteKafka {

    def main(args: Array[String]): Unit = {

        val c1javaSecurityKrb5Conf = KerberosConfigLoader.getString("c1.java.security.krb5.conf")
        val c1javaSecurityAuthLoginConfig = KerberosConfigLoader.getString("c1.java.security.auth.login.config")

        // 设置kerberos认证，本地模式和集群模式需要修改配置
        System.setProperty("java.security.krb5.conf", c1javaSecurityKrb5Conf)
        System.setProperty("java.security.auth.login.config", c1javaSecurityAuthLoginConfig)

        val spark: SparkSession = SparkSession
          .builder()
          .appName("ReadKafkaWriteKafka")
          .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=" + c1javaSecurityAuthLoginConfig)
          .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=" + c1javaSecurityAuthLoginConfig)
//          .master("local[*]")
          .getOrCreate()
        import spark.implicits._
        val sc: SparkContext = spark.sparkContext
        val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

        // 加载集群1配置文件
        val c1_brokers: String = KerberosConfigLoader.getString("c1.kafka.broker.list")
        val c1_topics: String = KerberosConfigLoader.getString("c1.kafka.topic.read")
        // 加载集群2配置文件
        val c2_brokers: String = KerberosConfigLoader.getString("c2.kafka.broker.list")
        val c2_topics: String = KerberosConfigLoader.getString("c2.kafka.topic.write")

        // 处理集群1Kafka topic字符串
        val c1_topics_set = c1_topics.split(",").toSet

        val kafkaParams = Map(
            "bootstrap.servers" -> c1_brokers, // 用于初始化链接到集群的地址
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
                p.setProperty("bootstrap.servers", c2_brokers)
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
            ConsumerStrategies.Subscribe[String, String](c1_topics_set, kafkaParams)
        )

        kafkaDStream.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                println("kafkaRDD is not Empty!^^!")
                rdd.foreachPartition(partitionRecords => {
                    partitionRecords.foreach(line => {
                        // 写入到Kafka
                        val sinkData: String = line.value()
                        kafkaProducer.value.send(c2_topics, sinkData)
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
