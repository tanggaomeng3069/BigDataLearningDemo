package com.learning.kerberos

import java.io.IOException

import com.learning.utils.KerberosConfigLoader
import org.apache.commons.configuration.plist.PropertyListConfiguration
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph


object SaprkOnKafkaWriteJanusGraph {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
        System.setProperty("java.security.krb5.conf", KerberosConfigLoader.getString("java.security.krb5.conf"))
        System.setProperty("java.security.auth.login.config", KerberosConfigLoader.getString("java.security.auth.login.config"))

        val configuration = new Configuration()
        configuration.set("hadoop.security.authentication", "Kerberos")
        configuration.set("hbase.security.authentication", "Kerberos")
        try {
            UserGroupInformation.setConfiguration(configuration)
            UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"))
            println("Kerberos认证成功: " + UserGroupInformation.getCurrentUser)
        } catch {
            case e: IOException => println("Kerberos认证失败。。。")
        }

        // 创建和配置SparkConf，如果是Yarn-Cluster模式提交任务，需要去掉 .setMaster("local[*]")
        val sparkConf: SparkConf = new SparkConf()
            .setAppName("SparkOnKafkaWriteTopic")
            .set("spark.driver.extraJavaOptions", s"-Djava.security.auth.login.config=" + KerberosConfigLoader.getString("java.security.auth.login.config"))
            .set("spark.executor.extraJavaOptions", s"-Djava.security.auth.login.config=" + KerberosConfigLoader.getString("java.security.auth.login.config"))
//            .setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        // 创建SparkStreaming并设置间隔时间10s
        val ssc = new StreamingContext(sc, Seconds(10))

        // 加载配置文件 ConfigLoader.getString("")
        val brokers: String = KerberosConfigLoader.getString("kafka.broker.list")
        val topics: String = KerberosConfigLoader.getString("kafka.topic")
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

        // yarn cluster 模式正常运行
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
        )
        kafkaDStream.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                println("kafkaRDD is not Empty!^^!")
                rdd.foreachPartition(partitionRecords => {
                    val janusConf = new PropertyListConfiguration()
                    janusConf.addProperty("clusterConfiguration.hosts", KerberosConfigLoader.getString("janusgraph.gremlin.server"))
                    janusConf.addProperty("clusterConfiguration.port", KerberosConfigLoader.getString("janusgraph.gremlin.port"))
                    janusConf.addProperty("clusterConfiguration.serializer.className", "org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0")
                    janusConf.addProperty("gremlin.remote.remoteConnectionClass", "org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection")
                    janusConf.addProperty("gremlin.remote.driver.sourceName", "g")
                    val g: GraphTraversalSource = EmptyGraph.instance.traversal.withRemote(janusConf)
                    var counts = 0L
                    partitionRecords.foreach(line => {
                        val strings: Array[String] = line.value().split(",")
                        val s1: String = strings(0).toString
                        val s2: Int = strings(1).toInt
                        val s3: String = strings(2).toString
                        g.addV(s1).property("property1", s2).property("property2", s3).next()
                        println("Insert JanusGraph Successfully !^^!")
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
