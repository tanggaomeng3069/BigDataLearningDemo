package com.learning.kerberos

import java.io.IOException
import java.security.PrivilegedAction
import java.util.UUID

import com.learning.utils.KerberosConfigLoader
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
import scala.util.parsing.json.JSON

/**
 * kafka输入数据格式：
 * {"action": "1","name": "Ambari","version": "V2.7.3","os": "CentOS","Core": "7.4.1708","mail": "bigdata@ambari.com"}
 */
object ReadTopicWriteHBase {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        val javaSecurityAuthLoginConfig = KerberosConfigLoader.getString("java.security.auth.login.config")

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
        System.setProperty("java.security.krb5.conf", KerberosConfigLoader.getString("java.security.krb5.conf"))
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

        val spark: SparkSession = SparkSession
            .builder()
            .appName("ReadTopicWriteHBase")
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .master("local[*]")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(10))

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

        kafkaDStream.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                println("kafkaRDD is not Empty!^^!")
                rdd.foreachPartition(partitionRecords => {
                    // 获取HBase连接
                    val hbaseConnection: Connection = getHBaseConn()
                    partitionRecords.foreach(line => {
                        println(line.value())
                        // 补充代码逻辑
                        // 连接HBase表
                        val tableName: TableName = TableName.valueOf(KerberosConfigLoader.getString("hbase.table.name"))
                        val table: Table = hbaseConnection.getTable(tableName)
                        // 将kafka的每一条消息解析为JSON格式数据
                        val jsonObj: Option[Any] = JSON.parseFull(line.value())
                        val data: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
                        val uuid: String = UUID.randomUUID().toString
                        val action: String = data("action").asInstanceOf[String]
                        val name: String = data("name").asInstanceOf[String]
                        val version: String = data("version").asInstanceOf[String]
                        val os: String = data("os").asInstanceOf[String]
                        val Core: String = data("Core").asInstanceOf[String]
                        val mail: String = data("mail").asInstanceOf[String]

                        val put = new Put(Bytes.toBytes(uuid))
                        val tableColumnFamily: String = KerberosConfigLoader.getString("hbase.table.column.family")
                        put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes("action"), Bytes.toBytes(action))
                        put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes("name"), Bytes.toBytes(name))
                        put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes("version"), Bytes.toBytes(version))
                        put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes("os"), Bytes.toBytes(os))
                        put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes("Core"), Bytes.toBytes(Core))
                        put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes("mail"), Bytes.toBytes(mail))
                        // 将数据写入HBase，若出错关闭table
                        Try(table.put(put)).getOrElse(table.close())
                        // 分区写入HBase后关闭连接
                        table.close()
                    })
                    hbaseConnection.close()
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

    def getHBaseConn(): Connection = {
        System.setProperty("java.security.krb5.conf", KerberosConfigLoader.getString("java.security.krb5.conf"))
        val conf: Configuration = HBaseConfiguration.create()

        // Kerberos认证
        conf.set("hbase.zookeeper.quorum", KerberosConfigLoader.getString("hbase.zookeeper.list"))
        conf.set("hbase.zookeeper.property.clientPort", KerberosConfigLoader.getString("hbase.zookeeper.port"))
        conf.set("hadoop.security.authentication", "Kerberos")
        conf.set("hbase.security.authentication", "Kerberos")
        conf.set("hbase.master.kerberos.principal", KerberosConfigLoader.getString("hbase.master.kerberos.principal"))
        conf.set("hbase.regionserver.kerberos.principal", KerberosConfigLoader.getString("hbase.regionserver.kerberos.principal"))
        conf.set("zookeeper.znode.parent", KerberosConfigLoader.getString("zookeeper.znode.parent"))
        conf.set("hbase.client.retries.number", KerberosConfigLoader.getString("hbase.client.retries.number")) // 设置重试次数

        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"))
        val loginUser: UserGroupInformation = UserGroupInformation.getLoginUser
        loginUser.doAs(new PrivilegedAction[Connection] {
            override def run(): Connection = ConnectionFactory.createConnection(conf)
        })
    }

}
