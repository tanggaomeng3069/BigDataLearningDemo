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
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object ReadHiveWriteHBase {

    def main(args: Array[String]): Unit = {

        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

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

        // 如果是Yarn-Cluster模式提交任务，需要去掉 .setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .appName("ReadHiveWriteHBase")
//            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        val dataFrame: DataFrame = spark.sql("select active,name,version,os,Core,mail from " + KerberosConfigLoader.getString("hive.table.name") + " limit 10")
        dataFrame.show()
        println("Query Hive Succeeded !^^!")

        dataFrame.rdd.foreachPartition(partition => {
            // 获取HBase连接
            val hbaseConnection: Connection = getHBaseConn()
            partition.foreach(line => {
                // 连接HBase表
                val tableName: TableName = TableName.valueOf(KerberosConfigLoader.getString("hbase.table.name"))
                val table: Table = hbaseConnection.getTable(tableName)
                val uuid: String = UUID.randomUUID().toString
                val active: String = line.getAs[String]("active")
                val name: String = line.getAs[String]("name")
                val version: String = line.getAs[String]("version")
                val os: String = line.getAs[String]("os")
                val Core: String = line.getAs[String]("Core")
                val mail: String = line.getAs[String]("mail")
                val put = new Put(Bytes.toBytes(uuid))
                val tableColumnFamily: String = KerberosConfigLoader.getString("hbase.table.column.family")
                put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes("active"), Bytes.toBytes(active))
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

        println("Write to HBase Succeeded !^^!")

        spark.stop()

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
