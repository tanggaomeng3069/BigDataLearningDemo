package com.learning.kerberos

import java.io.IOException
import java.util.Properties

import com.learning.utils.{KafkaSink, KerberosConfigLoader}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadHBaseWriteKafka {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        val javaSecurityAuthLoginConfig = KerberosConfigLoader.getString("java.security.auth.login.config")

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
        System.setProperty("java.security.krb5.conf", KerberosConfigLoader.getString("java.security.krb5.conf"))
        System.setProperty("java.security.auth.login.config", javaSecurityAuthLoginConfig)

        // Kerberos认证
        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", KerberosConfigLoader.getString("hbase.zookeeper.list"))
        conf.set("hbase.zookeeper.property.clientPort", KerberosConfigLoader.getString("hbase.zookeeper.port"))
        conf.set("hadoop.security.authentication", "Kerberos")
        conf.set("hbase.security.authentication", "Kerberos")
        conf.set("hbase.master.kerberos.principal", KerberosConfigLoader.getString("hbase.master.kerberos.principal"))
        conf.set("hbase.regionserver.kerberos.principal", KerberosConfigLoader.getString("hbase.regionserver.kerberos.principal"))
        conf.set("zookeeper.znode.parent", KerberosConfigLoader.getString("zookeeper.znode.parent"))
        conf.set("hbase.client.retries.number", KerberosConfigLoader.getString("hbase.client.retries.number")) // 设置重试次数
        try {
            UserGroupInformation.setConfiguration(conf)
            UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"))
            println("Kerberos认证成功: " + UserGroupInformation.getCurrentUser)
        } catch {
            case e: IOException => println("Kerberos认证失败。。。")
        }

        // 加载配置文件
        val brokers: String = KerberosConfigLoader.getString("kafka.broker.list")
        val topics: String = KerberosConfigLoader.getString("kafka.topic.write")
        if (StringUtils.isEmpty(brokers) || StringUtils.isEmpty(topics)) {
            println("未配置 Kafka 信息...")
            System.exit(0)
        }

        val spark: SparkSession = SparkSession
            .builder()
            .appName("ReadHBaseWriteKafka")
            .master("local[*]")
            .config("spark.driver.extraJavaOptions", s"-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .config("spark.executor.extraJavaOptions", s"-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext

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

        val tableName: String = KerberosConfigLoader.getString("hbase.table.name")

        try {
            val hbaseContext = new HBaseContext(sc, conf)
            val scan = new Scan()
            val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)

            hbaseRDD.foreachPartition(partition => {
                partition.foreach(line => {
                    // 写入到Kafka
                    val sinkData: String = "rowKey: " + Bytes.toString(line._1.get())
                    kafkaProducer.value.send(topics, sinkData)
                })
            })

        } finally {
            sc.stop()
        }

        spark.close()


    }

}
