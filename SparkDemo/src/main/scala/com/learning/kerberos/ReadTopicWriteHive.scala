package com.learning.kerberos

import java.io.IOException

import com.learning.utils.KerberosConfigLoader
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Kafka中数据格式：1,Ambari,V2.7.3,CentOS,7.4.1708,bigdata@ambari.com
 */
object ReadTopicWriteHive {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        val javaSecurityKrb5Conf = KerberosConfigLoader.getString("java.security.krb5.conf")
        val javaSecurityAuthLoginConfig = KerberosConfigLoader.getString("java.security.auth.login.config")

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
        System.setProperty("java.security.krb5.conf", javaSecurityKrb5Conf)
        System.setProperty("java.security.auth.login.config", javaSecurityAuthLoginConfig)

        val configuration = new Configuration()
        configuration.set("hadoop.security.authentication", "Kerberos")
        try {
            UserGroupInformation.setConfiguration(configuration)
            UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"))
            println("Kerberos认证成功: " + UserGroupInformation.getCurrentUser)
        } catch {
            case e: IOException => println("Kerberos认证失败。。。")
        }

        // 如果是Yarn-Cluster模式提交任务，需要去掉 .setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .appName("SparkOnKafkaWriteHive")
            .master("local[*]")
            .config("spark.driver.extraJavaOptions", s"-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .config("spark.executor.extraJavaOptions", s"-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .enableHiveSupport()
            .getOrCreate()
        // 创建SparkStreaming并设置间隔时间10s
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

        // 加载配置文件 ConfigLoader.getString("")
        val brokers: String = KerberosConfigLoader.getString("kafka.broker.list")
        val topics: String = KerberosConfigLoader.getString("kafka.topic.read")
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

        // 样例类
        case class Person(name: String, version: String, mail: String)
        kafkaDStream.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                println("kafkaRDD is not Empty!^^!")
                val schema: StructType = types.StructType(
                    Seq(
                        StructField("active", StringType, nullable = true),
                        StructField("name", StringType, nullable = true),
                        StructField("version", StringType, nullable = true),
                        StructField("os", StringType, nullable = true),
                        StructField("Core", StringType, nullable = true),
                        StructField("mail", StringType, nullable = true)
                    )
                )
                val rowRDD: RDD[Row] = rdd.map(x => {
                    val value: Array[String] = x.value().toString.split(",")
                    Row(value(0), value(1), value(2), value(3), value(4), value(5))
                })
                val frame: DataFrame = spark.createDataFrame(rowRDD, schema)
                frame.createOrReplaceGlobalTempView("test")
                spark.sql("use " + KerberosConfigLoader.getString("hive.db.name"))
                frame.write.mode(SaveMode.Append).format("hive").saveAsTable(KerberosConfigLoader.getString("hive.table.name"))
                println("Insert Hive Successfully !^^!")
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
