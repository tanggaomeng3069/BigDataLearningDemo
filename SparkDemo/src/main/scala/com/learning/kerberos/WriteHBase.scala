package com.learning.kerberos

import java.io.IOException
import java.security.PrivilegedAction
import java.util.UUID

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import scala.util.Try

object WriteHBase {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        val javaSecurityAuthLoginConfig = KerberosConfigLoader.getString("java.security.auth.login.config")

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
        System.setProperty("java.security.krb5.conf", KerberosConfigLoader.getString("java.security.krb5.conf"))

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
            .appName("WriteHBase")
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .master("local[*]")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext
        // 示例数据
        val dataRDD: RDD[(String, String)] = sc.makeRDD(List(
            ("action", "1"), ("name", "Ambari"), ("version", "V2.7.3"), ("os", "CentOS"), ("Core", "7.4.1708"), ("mail", "bigdata@ambari.com")
        ))

        // 写入HBase
        dataRDD.foreachPartition(partition => {
            // 获取HBase连接
            val hbaseConnection: Connection = getHBaseConn()
            partition.foreach(line => {
                // 连接HBase表
                val tableName: TableName = TableName.valueOf(KerberosConfigLoader.getString("hbase.table.name"))
                val table: Table = hbaseConnection.getTable(tableName)
                val uuid: String = UUID.randomUUID().toString

                val put = new Put(Bytes.toBytes(uuid))
                val tableColumnFamily: String = KerberosConfigLoader.getString("hbase.table.column.family")
                put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes(line._1.toString), Bytes.toBytes(line._2.toString))

                // 将数据写入HBase，若出错关闭table
                Try(table.put(put)).getOrElse(table.close())
                // 分区写入HBase后关闭连接
                table.close()
            })
            hbaseConnection.close()
        })

        sc.stop()
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
