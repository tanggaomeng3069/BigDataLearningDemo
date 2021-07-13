package com.learning.kerberos

import java.util.Properties

import com.learning.utils.{KafkaSink, KerberosConfigLoader}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object WriteTopic {

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
//            .master("local[*]")
            .getOrCreate()

        val sc: SparkContext = spark.sparkContext
        // 示例数据
        val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

        // 加载配置文件
        val brokers: String = KerberosConfigLoader.getString("c2.kafka.broker.list")
        val topicWrite: String = KerberosConfigLoader.getString("c2.kafka.topic.write")
        if (StringUtils.isEmpty(brokers) || StringUtils.isEmpty(topicWrite)) {
            println("未配置 Kafka 信息...")
            System.exit(0)
        }
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
            sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
        }

        // 写入Kafka
        dataRDD.foreachPartition(partition => {
            partition.foreach(line => {
                val sinkData: String = line.toString
                kafkaProducer.value.send(topicWrite, sinkData)
            })
        })

        sc.stop()
    }

}
